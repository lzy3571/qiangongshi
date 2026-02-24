import pandas as pd
from sqlalchemy import func
from database import init_db, Session, Mechanic, RewardHistory, Issue, MonthlyRecord, ClearedHoursRecord, Attachment6Data, Attachment1Data
import os

def safe_float(val):
    try:
        if pd.isna(val):
            return 0.0
        val_str = str(val).strip()
        if not val_str or val_str == '/':
            return 0.0
        return float(val_str)
    except:
        return 0.0

import datetime

def excel_date_to_str(val):
    """
    Convert Excel serial date to YYYY-MM format.
    Excel stores dates as float (e.g. 45444 = 2024-06-01 approx).
    """
    try:
        if pd.isna(val):
            return ""
        
        val_str = str(val).strip()
        if not val_str:
            return ""
        
        # Check if it's a number (int or float)
        # Sometimes pandas reads it as string "45444" or float 45444.0
        try:
            val_float = float(val)
            # Excel base date: 1899-12-30
            # Some dates might be "YYYYMM" format like "202407"? 
            # If val > 40000, likely serial date.
            # If val roughly 202400, likely YYYYMM.
            
            if val_float > 40000 and val_float < 50000:
                dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=val_float)
                return dt.strftime('%Y.%m')
            elif val_float > 202000 and val_float < 203000: # YYYYMM
                # e.g. 202407
                val_s = str(int(val_float))
                if len(val_s) >= 6:
                     return f"{val_s[:4]}.{val_s[4:6]}"
            
        except ValueError:
            pass # Not a number
            
        # Try parsing string formats
        # "2024年7月"
        if '年' in val_str and '月' in val_str:
            val_str = val_str.replace('年', '.').replace('月', '')
            
        # "2024.7" or "2024-7"
        val_str = val_str.replace('-', '.')
        if '.' in val_str:
             parts = val_str.split('.')
             if len(parts) >= 2:
                  return f"{parts[0]}.{int(parts[1]):02d}"
        
        return val_str
    except Exception as e:
        return str(val)

def safe_int(val):
    try:
        return int(safe_float(val))
    except:
        return 0

from datetime import datetime
import re

def parse_date_from_text(text):
    """Attempt to extract date from problem description"""
    if not isinstance(text, str):
        return None
    
    # Common patterns: YYYY年M月D日, YYYY/M/D, YYYY.M.D
    patterns = [
        r'(\d{4})年(\d{1,2})月(\d{1,2})日',
        r'(\d{4})/(\d{1,2})/(\d{1,2})',
        r'(\d{4})\.(\d{1,2})\.(\d{1,2})'
    ]
    
    for p in patterns:
        match = re.search(p, text)
        if match:
            y, m, d = match.groups()
            return f"{y}-{int(m):02d}-{int(d):02d}"
            
    # Try finding "M月D日" and assume current/previous year? 
    # Too risky. Just return None if no full date found.
    return None

def load_initial_data():
    init_db()
    session = Session()
    
    # ... (Phase 1 remains same)
    file_att6 = "使用文件/附件6 随车机械师积分及千工时保安全竞赛奖励明细汇总表.xlsx"
    if os.path.exists(file_att6):
        # Phase 1: Load mechanics from Attachment 6 (History)
        # This file contains historical reward data.
        # Columns: 序号, 动车所, 姓名, 工号, 乘务组别, 活动起累计工时, 本次满足奖励 时间（月份）, 本次奖励周期, 本次奖励周期内千工时累积扣分, 奖励金额, 累计奖励清零次数, 累计清零工时, 备注
        
        print("Loading data from Attachment 6 (History)...")
        # Read header=2 (Row 3 is header) based on file inspection usually
        # But previous code used sheet_name=0 and default header=0?
        # Let's try header=1 (Row 2) or header=2 (Row 3).
        # User said "Header includes...".
        # Let's inspect rows first.
        # Usually Attachment 6 has Title (Row 1), Header (Row 2 or 3).
        # Let's try header=2 (Index 2, Row 3).
        try:
            df = pd.read_excel(file_att6, sheet_name=0, header=2)
            # Check if columns match expected
            if '工号' not in df.columns:
                # Fallback to header=1
                df = pd.read_excel(file_att6, sheet_name=0, header=1)
            
            df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
            
            session.query(RewardHistory).delete()
            session.query(Attachment6Data).delete() # Clear raw data table
            session.commit()
            
            count_history = 0
            for _, row in df.iterrows():
                if pd.isna(row.get('工号')): continue
                
                emp_id = str(row.get('工号')).strip()
                if emp_id.endswith('.0'): emp_id = emp_id[:-2]
                
                mechanic = session.query(Mechanic).filter_by(employee_id=emp_id).first()
                if not mechanic:
                    mechanic = Mechanic(employee_id=emp_id)
                    session.add(mechanic)
                
                mechanic.name = str(row.get('姓名', '')).strip()
                mechanic.team = str(row.get('乘务组别', '')).strip()
                
                # Helper for cleaning values
                def clean_str(val):
                    s = str(val).strip()
                    if s == 'nan' or s == '/': return ""
                    return s
                
                def clean_float(val):
                    try:
                        s = str(val).strip()
                        if s == '/' or s == 'nan' or not s: return 0.0
                        return float(s)
                    except:
                        return 0.0

                def clean_int(val):
                    try:
                        s = str(val).strip()
                        if s == '/' or s == 'nan' or not s: return 0
                        return int(float(s))
                    except:
                        return 0

                # 1. Add to RewardHistory (Linked to Mechanic)
                history = RewardHistory(mechanic=mechanic)
                history.reward_date = excel_date_to_str(row.get('本次满足奖励 时间（月份）'))
                history.reward_cycle = clean_str(row.get('本次奖励周期'))
                history.amount = clean_float(row.get('奖励金额'))
                
                # Deduction sign check
                raw_ded = clean_float(row.get('本次奖励周期内千工时累积扣分'))
                if raw_ded > 0:
                    history.deduction = -raw_ded
                else:
                    history.deduction = raw_ded
                    
                history.cleared_hours = clean_float(row.get('累计清零工时'))
                
                history.extra_reward = clean_float(row.get('本次连续额外奖励'))
                history.total_amount = clean_float(row.get('本月奖励金额（元）'))
                history.consecutive_info = clean_str(row.get('连续奖励情况 （上一周期奖励）')) 
                # Note: Column name might vary with newline
                # Try variations
                if not history.consecutive_info:
                     history.consecutive_info = clean_str(row.get('连续奖励情况\n（上一周期奖励）'))

                session.add(history)
                
                # 2. Add to Attachment6Data (Raw Data Management)
                att6 = Attachment6Data(
                    employee_id=emp_id,
                    name=mechanic.name,
                    team=mechanic.team,
                    reward_date=history.reward_date,
                    reward_cycle=history.reward_cycle,
                    reward_amount=history.amount,
                    
                    activity_cumulative_hours=clean_float(row.get('活动起累计工时')),
                    cycle_deduction=history.deduction,
                    cleared_hours=history.cleared_hours,
                    
                    # New Fields
                    balance_hours=clean_float(row.get('当前前结余工时')),
                    past_reward_count=clean_int(row.get('过去已奖励次数')),
                    is_consecutive=clean_str(row.get('本次奖励是否触发连续奖励')),
                    consecutive_info=history.consecutive_info,
                    extra_reward=history.extra_reward,
                    total_amount=history.total_amount
                )
                session.add(att6)
                
                count_history += 1
                
            print(f"Loaded {count_history} reward history records.")
        except Exception as e:
            print(f"Error loading Attachment 6: {e}")
            import traceback
            traceback.print_exc()

    # --- Phase 2: Load Initial Hours AND Issues from Attachment 2 ---
    file_att2 = "使用文件/附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
    if os.path.exists(file_att2):
        print("Loading initial hours and issues from Attachment 2...")
        try:
            # Note: Sheet name has a trailing space based on inspection
            df_hours = pd.read_excel(file_att2, sheet_name="随车机械师工时奖励周期内积分统计 ", header=3)
            # Columns: 序号, 姓名, 工号, 乘务组别, 统计周期, 累计工时, 竞赛周期内扣分明细(G), 问题来源(H), 扣分条款(I), 扣分明细(J), 扣分总计(K)
            
            count_hours = 0
            count_issues = 0
            
            # Clear existing issues to avoid duplication on reload
            session.query(Issue).delete()
            session.commit()
            
            # The structure of the Excel is:
            # Row N: Mechanic Info (Name, ID, Hours...) + Optional Issue 1
            # Row N+1 to N+9: Empty Name/ID, but potentially Issue 2, Issue 3...
            # We need to track "Current Mechanic" as we iterate.
            
            current_mechanic_id = None
            
            for index, row in df_hours.iterrows():
                # Check if this row has a mechanic definition (Name/ID present)
                # Note: '工号' might be NaN for continuation rows
                
                emp_id_raw = row.get('工号')
                name_raw = row.get('姓名')
                
                if pd.notna(emp_id_raw):
                    # New Mechanic Block
                    emp_id = str(emp_id_raw).strip()
                    if emp_id.endswith('.0'): emp_id = emp_id[:-2]
                    
                    mechanic = session.query(Mechanic).filter_by(employee_id=emp_id).first()
                    if not mechanic:
                        mechanic = Mechanic(employee_id=emp_id)
                        # Clean name (remove extra spaces if needed, but usually keep as is)
                        mechanic.name = str(name_raw).strip() if pd.notna(name_raw) else ''
                        mechanic.team = str(row.get('乘务组别', '')).strip()
                        session.add(mechanic)
                        session.flush() # Flush to get ID
                    
                    current_mechanic_id = mechanic.id
                    
                    # Update Hours (only on the main row)
                    raw_hours = safe_float(row.get('累计工时'))
                    
                    # Store as base_hours (Static snapshot Dec 2025)
                    mechanic.base_hours = raw_hours
                    
                    # Calculate Total Hours (Base + All Monthly Imports)
                    if raw_hours > 1000:
                         mechanic.base_hours = raw_hours % 1000
                    else:
                         mechanic.base_hours = raw_hours
                    
                    # Initialize total_hours
                    monthly_sum = session.query(func.sum(MonthlyRecord.hours)).filter_by(mechanic_id=mechanic.id).scalar() or 0.0
                    mechanic.total_hours = mechanic.base_hours + monthly_sum

                    mechanic.current_cycle_deduction = 0.0
                    
                    count_hours += 1
                
                # Process Issue (on EVERY row if current_mechanic_id is set)
                if current_mechanic_id:
                    problem = str(row.get('竞赛周期内扣分明细', ''))
                    # Check if valid problem
                    if pd.notna(row.get('竞赛周期内扣分明细')) and problem != 'nan' and problem.strip():
                        deduction = safe_float(row.get('扣分明细'))
                        total_deduction = safe_float(row.get('扣分总计'))
                        
                        date_str = parse_date_from_text(problem)
                        if not date_str:
                            date_str = "2025-12-25" # Fallback
                        
                        issue = Issue(
                            mechanic_id=current_mechanic_id,
                            date=date_str,
                            problem=problem,
                            source=str(row.get('问题来源', '')),
                            clause=str(row.get('扣分条款', '')),
                            detail=deduction,
                            total_deduction=total_deduction,
                            status='未结算'
                        )
                        session.add(issue)
                        count_issues += 1

            print(f"Updated hours for {count_hours} mechanics.")
            print(f"Loaded {count_issues} historical issues.")
            
        except Exception as e:
            print(f"Error loading Attachment 2: {e}")
            import traceback
            traceback.print_exc()

    # --- Phase 3: Load Cleared Hours from Attachment 8 ---
    file_att8 = "使用文件/附件8 随车机械师积分及“千工时”保安全竞赛工时奖励清零汇总表.xlsx"
    if os.path.exists(file_att8):
        print("Loading cleared hours from Attachment 8...")
        try:
            df_cleared = pd.read_excel(file_att8, header=1) # Header is row 2 (Index 1)
            # Columns: 序号, 动车所, 姓名, 工号, 乘务组别, 活动起累计工时, 本次满足奖励\n时间（月份）, 本次奖励周期, 本次奖励周期内千工时累积扣分, 本次奖励周期内千工时扣分明细, 累计奖励清零次数, 累计清零工时, 备注
            
            session.query(ClearedHoursRecord).delete()
            session.commit()
            
            count_cleared = 0
            
            # Need to handle multi-row deduction details
            # Structure: 
            # Row N: Main Record
            # Row N+1..M: Continuation of Deduction Details (Name/ID empty)
            
            current_record = None
            
            for _, row in df_cleared.iterrows():
                emp_id_raw = row.get('工号')
                
                if pd.notna(emp_id_raw):
                    # New Record
                    emp_id = str(emp_id_raw).strip()
                    if emp_id.endswith('.0'): emp_id = emp_id[:-2]
                    
                    # Find mechanic (Create if needed, logic from before)
                    mechanic = session.query(Mechanic).filter_by(employee_id=emp_id).first()
                    if not mechanic:
                         mechanic = Mechanic(employee_id=emp_id)
                         mechanic.name = str(row.get('姓名', '')).strip()
                         mechanic.team = str(row.get('乘务组别', '')).strip()
                         session.add(mechanic)
                         session.flush()

                    base_val = safe_float(row.get('累计清零工时'))
                    
                    record = ClearedHoursRecord(
                        mechanic_id=mechanic.id,
                        depot=str(row.get('动车所', '')).strip(),
                        name=str(row.get('姓名', '')).strip(),
                        employee_id=emp_id,
                        team=str(row.get('乘务组别', '')).strip(),
                        activity_cumulative_hours=safe_float(row.get('活动起累计工时')),
                        reward_month=excel_date_to_str(row.get('本次满足奖励\n时间（月份）')),
                        reward_cycle=str(row.get('本次奖励周期', '')).strip(),
                        cycle_deduction=safe_float(row.get('本次奖励周期内千工时累积扣分')),
                        deduction_details=str(row.get('本次奖励周期内千工时扣分明细', '')).strip(),
                        clearing_count=safe_int(row.get('累计奖励清零次数')),
                        
                        # New Columns Logic
                        cleared_hours_base=base_val,
                        cleared_hours_new=0.0,
                        total_cleared_hours=base_val, # Initially Base + 0
                        
                        remarks=str(row.get('备注', '')) if pd.notna(row.get('备注')) else ''
                    )
                    
                    # Fix: If main row's deduction detail is empty, don't store "nan" or ""
                    # The raw get() might return NaN, which str() converts to "nan".
                    # We should clean it.
                    detail_str = str(row.get('本次奖励周期内千工时扣分明细', ''))
                    if pd.isna(row.get('本次奖励周期内千工时扣分明细')) or detail_str == 'nan' or not detail_str.strip():
                        record.deduction_details = ""
                    else:
                        record.deduction_details = detail_str.strip()

                    session.add(record)
                    current_record = record
                    count_cleared += 1
                
                elif current_record:
                    # Continuation Row - Append Deduction Details
                    detail_val = row.get('本次奖励周期内千工时扣分明细')
                    detail_part = str(detail_val)
                    # IMPORTANT: Only append if it's not nan/empty AND not just whitespace
                    if pd.notna(detail_val) and detail_part != 'nan' and detail_part.strip():
                         # Ensure we don't start with newline if previous was empty
                         if current_record.deduction_details:
                             current_record.deduction_details += "\n" + detail_part.strip()
                         else:
                             current_record.deduction_details = detail_part.strip()
            
            print(f"Loaded {count_cleared} cleared hours records.")
            
        except Exception as e:
            print(f"Error loading Attachment 8: {e}")
            import traceback
            traceback.print_exc()

    # --- Phase 4: Load Attachment 1 (Deduction List) ---
    file_att1 = "使用文件/附件1 随车机械师积分制管理扣分清单.xlsx"
    if os.path.exists(file_att1):
        print("Loading deduction list from Attachment 1...")
        try:
            # Header is likely on row 1 (0-indexed)
            df_att1 = pd.read_excel(file_att1, header=1)
            # Columns: 序号, 扣分大类, 具体项目, 扣分分值, 详细描述, 备注
            
            session.query(Attachment1Data).delete()
            session.commit()
            
            count_att1 = 0
            for _, row in df_att1.iterrows():
                if pd.isna(row.get('序号')): continue
                
                att1 = Attachment1Data(
                    source=str(row.get('扣分大类', '')).strip(),
                    clause=str(row.get('具体项目', '')).strip(),
                    detail=str(row.get('详细描述', '')).strip(),
                    score=safe_float(row.get('扣分分值'))
                )
                session.add(att1)
                count_att1 += 1
            
            print(f"Loaded {count_att1} deduction rules from Attachment 1.")
            
        except Exception as e:
            print(f"Error loading Attachment 1: {e}")
            import traceback
            traceback.print_exc()

    session.commit()
    session.close()

if __name__ == "__main__":
    load_initial_data()
