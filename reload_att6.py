import os
import pandas as pd
from datetime import datetime
from database import Session, Attachment6Data, RewardHistory, Mechanic

FILE_PATH = "使用文件/附件6 随车机械师积分及千工时保安全竞赛奖励明细汇总表.xlsx"

def excel_date_to_str(val):
    try:
        if pd.isna(val): return ""
        val_str = str(val).strip()
        if not val_str: return ""
        try:
            val_float = float(val)
            if val_float > 40000 and val_float < 50000:
                dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(val_float) - 2)
                return dt.strftime('%Y.%m')
            elif val_float > 202000 and val_float < 203000:
                val_s = str(int(val_float))
                if len(val_s) >= 6: return f"{val_s[:4]}.{val_s[4:6]}"
        except: pass
        if '年' in val_str and '月' in val_str:
            val_str = val_str.replace('年', '.').replace('月', '')
        val_str = val_str.replace('-', '.')
        if '.' in val_str:
            parts = val_str.split('.')
            if len(parts) >= 2: return f"{parts[0]}.{int(parts[1]):02d}"
        return val_str
    except: return str(val)

def clean_str(val):
    s = str(val).strip()
    if s == 'nan' or s == '/': return ""
    return s

def clean_float(val):
    try:
        s = str(val).strip()
        if s == '/' or s == 'nan' or not s: return 0.0
        return float(s)
    except: return 0.0

def clean_int(val):
    try:
        s = str(val).strip()
        if s == '/' or s == 'nan' or not s: return 0
        return int(float(s))
    except: return 0

def reload_att6():
    print(f"Loading data from {FILE_PATH}...")
    session = Session()
    try:
        # 1. Find header row
        try:
            df_preview = pd.read_excel(FILE_PATH, sheet_name=0, header=None, nrows=10)
        except FileNotFoundError:
            print(f"File not found: {FILE_PATH}")
            return
            
        header_row_idx = -1
        for i, row in df_preview.iterrows():
            row_values = [str(val).strip() for val in row.values]
            if '工号' in row_values:
                header_row_idx = i
                break
        
        if header_row_idx == -1:
            header_row_idx = 2 # Default fallback
            
        print(f"Detected header row index: {header_row_idx}")
        
        # 2. Read data
        df = pd.read_excel(FILE_PATH, sheet_name=0, header=header_row_idx)
        
        # 3. Normalize columns
        original_cols = df.columns.tolist()
        df.columns = [str(c).replace('\n', '').replace(' ', '').replace('\u3000', '').replace('\t', '').strip() for c in df.columns]
        print(f"Columns normalized: {df.columns.tolist()}")
        
        # Check for '工号'
        id_col = '工号'
        if id_col not in df.columns:
            fuzzy = next((c for c in df.columns if '工号' in c), None)
            if fuzzy:
                print(f"Renaming fuzzy column '{fuzzy}' to '工号'")
                df.rename(columns={fuzzy: '工号'}, inplace=True)
            else:
                print("CRITICAL ERROR: Column '工号' not found!")
                return

        # 4. Clear existing data
        print("Clearing existing Attachment 6 data...")
        session.query(RewardHistory).delete()
        session.query(Attachment6Data).delete()
        session.flush()
        session.commit()
        
        # 5. Insert data
        count = 0
        for idx, row in df.iterrows():
            if pd.isna(row.get('工号')): continue
            
            emp_id = str(row.get('工号')).strip()
            if emp_id.endswith('.0'): emp_id = emp_id[:-2]
            if not emp_id: continue

            mechanic = session.query(Mechanic).filter_by(employee_id=emp_id).first()
            if not mechanic:
                mechanic = Mechanic(employee_id=emp_id)
                session.add(mechanic)
            
            mechanic.name = str(row.get('姓名', '')).strip()
            mechanic.team = str(row.get('乘务组别', '')).strip()
            
            history = RewardHistory(mechanic=mechanic)
            
            # Map columns
            # Try variations for date
            r_date_col = next((c for c in df.columns if '本次满足奖励时间' in c), None)
            history.reward_date = excel_date_to_str(row.get(r_date_col)) if r_date_col else ""
            
            history.reward_cycle = clean_str(row.get('本次奖励周期'))
            history.amount = clean_float(row.get('奖励金额'))
            
            # Forced Negative Deduction
            ded_col = next((c for c in df.columns if '千工时累积扣分' in c or '本次奖励周期内扣分' in c), None)
            raw_ded = 0.0
            if ded_col:
                raw_ded = clean_float(row.get(ded_col))
            else:
                print(f"Warning: Deduction column not found for row {idx}")
            
            # User Feedback: "Row 4 data is -0.5, but Excel says -1. Did you tamper?"
            # Issue: clean_float might be doing something weird? Or index mismatch?
            # Or maybe clean_float returns 0.0 for some string formats?
            # Let's check if raw_ded is what we expect.
            # Wait, user said "In Excel G5 cell, value should be -1".
            # If our clean_float logic is:
            # -abs(raw_ded)
            # If raw_ded is -0.5, result is -0.5.
            # Why did user see -0.5? Maybe raw data IS -0.5?
            # Or maybe row index is off? Row 4 in Excel (Header=0) -> Index 3?
            # User says "4th record".
            
            # Revert Forced Negative logic slightly? No, user wanted negative.
            # But user says value is wrong (-0.5 vs -1).
            # This implies read error or rounding?
            # Or maybe duplicate names?
            
            final_ded = -abs(raw_ded)
            if final_ded == 0: final_ded = 0.0
            history.deduction = final_ded
            
            history.cleared_hours = clean_float(row.get('累计清零工时'))
            
            # Try fuzzy match for cleared hours if not found
            if not history.cleared_hours:
                cleared_col = next((c for c in df.columns if '累计清零工时' in c), None)
                if cleared_col:
                     history.cleared_hours = clean_float(row.get(cleared_col))

            history.extra_reward = clean_float(row.get('本次连续额外奖励'))
            
            # Match Amount Column
            # Possible names: "本次奖励周期奖励金额", "奖励金额", "本月奖励金额"
            # In code above, history.amount was read from '奖励金额' or '本次奖励周期奖励金额'
            # Let's verify history.amount
            
            amt_col_1 = next((c for c in df.columns if '本次奖励周期奖励金额' in c), None)
            if amt_col_1:
                 history.amount = clean_float(row.get(amt_col_1))
            else:
                 # Fallback to just '奖励金额'
                 amt_col_2 = next((c for c in df.columns if '奖励金额' in c and '本月' not in c), None)
                 if amt_col_2:
                     history.amount = clean_float(row.get(amt_col_2))
            
            if row.get('姓名') == '金超':
                print(f"DEBUG CHECK: Name={row.get('姓名')}, Cycle={row.get('本次奖励周期')}, RawVal={row.get(ded_col)}, Parsed={raw_ded}, Final={final_ded}, ClearedHours={history.cleared_hours}, Amount={history.amount}")
            
            # 本月奖励金额 = 奖励金额 + 本次额外奖励
            # history.total_amount = clean_float(row.get(amt_col)) if amt_col else 0.0
            # User suggests: Just use DB col "本月奖励金额", AND it should equal amount + extra.
            
            amt_col = next((c for c in df.columns if '本月奖励金额' in c), None)
            if amt_col:
                history.total_amount = clean_float(row.get(amt_col))
            else:
                # Fallback calculation if column missing?
                history.total_amount = history.amount + history.extra_reward
            
            con_col = next((c for c in df.columns if '连续奖励情况' in c), None)
            history.consecutive_info = clean_str(row.get(con_col)) if con_col else ""
            
            session.add(history)
            
            att6 = Attachment6Data(
                employee_id=emp_id,
                name=mechanic.name,
                team=mechanic.team,
                reward_date=history.reward_date,
                reward_cycle=history.reward_cycle,
                reward_amount=history.amount,
                activity_cumulative_hours=clean_float(row.get('活动起累计工时')),
                cycle_deduction=final_ded,
                cleared_hours=history.cleared_hours,
                balance_hours=clean_float(row.get('当前前结余工时')),
                past_reward_count=clean_int(row.get('过去已奖励次数')),
                is_consecutive=clean_str(row.get('本次奖励是否触发连续奖励')),
                consecutive_info=history.consecutive_info,
                extra_reward=history.extra_reward,
                total_amount=history.total_amount
            )
            session.add(att6)
            count += 1
            
        session.commit()
        print(f"Successfully reloaded {count} records from Attachment 6.")
        
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    reload_att6()
