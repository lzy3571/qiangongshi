import os
import re
import pandas as pd
from database import Session, Mechanic, MonthlyRecord, Issue
from sqlalchemy import func

UPLOAD_FOLDER = 'uploads'

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

def reimport_uploads():
    print("Starting re-import of uploaded files...")
    session = Session()
    
    # 1. Clear MonthlyRecord table to avoid duplicates (since we re-calculate everything)
    # Wait, init_data.py DOES NOT clear MonthlyRecord.
    # So if we run init_data.py, MonthlyRecord is preserved.
    # BUT total_hours is reset to base + sum(MonthlyRecord).
    # This means total_hours is correct IF MonthlyRecord is correct.
    # BUT Issues from imports are LOST because init_data clears Issue table.
    # So we MUST re-import Issues.
    # And we MUST update current_cycle_deduction based on re-imported issues.
    
    # Strategy:
    # - MonthlyRecord: Preserved by init_data.py. So hours are fine.
    # - Issues: Cleared by init_data.py (except Att 2). Need to re-import.
    # - Deduction: init_data sets it to 0. We need to sum up Att 2 issues (if any) + Import Issues.
    
    # Step 1: Recalculate deduction from Att 2 issues (which are already loaded by init_data)
    # init_data adds issues but doesn't seem to update mechanic.current_cycle_deduction?
    # Let's check init_data.py... It sets mechanic.current_cycle_deduction = 0.0 initially.
    # Then it adds issues. It DOES NOT update current_cycle_deduction.
    # So we need to fix that first: Sum up existing issues for each mechanic.
    
    mechanics = session.query(Mechanic).all()
    for m in mechanics:
        m.current_cycle_deduction = 0.0
        for issue in m.issues:
            m.current_cycle_deduction += abs(issue.detail or 0.0)
    session.commit()
    print("Recalculated deductions from initial data.")
    
    # Step 2: Re-import Issues from 'uploads/'
    # We only need to process "issues_*.xlsx" files.
    # Hours are already in MonthlyRecord (persisted).
    
    files = os.listdir(UPLOAD_FOLDER)
    issue_files = [f for f in files if f.startswith('issues_') and f.endswith('.xlsx')]
    
    print(f"Found {len(issue_files)} issue files to process.")
    
    for f in issue_files:
        # Extract month from filename: issues_YYYY-MM_filename.xlsx
        # Regex: issues_(\d{4}-\d{2})_.*
        match = re.match(r'issues_(\d{4}-\d{2})_.*', f)
        if match:
            month = match.group(1)
            file_path = os.path.join(UPLOAD_FOLDER, f)
            print(f"Processing issues for {month} from {f}...")
            
            try:
                df = pd.read_excel(file_path)
                df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
                
                count = 0
                for _, row in df.iterrows():
                    name = str(row.get('姓名', '')).strip()
                    mechanic = session.query(Mechanic).filter_by(name=name).first()
                    if not mechanic: continue
                    
                    deduction = safe_float(row.get('扣分明细'))
                    deduction_mag = abs(deduction)
                    problem = str(row.get('问题', ''))
                    date_str = str(row.get('检查日期', ''))
                    
                    # Check duplicate
                    dup = session.query(Issue).filter_by(
                        mechanic_id=mechanic.id,
                        problem=problem,
                        detail=deduction
                    ).first()
                    
                    if dup: continue
                    
                    # Create Issue
                    issue = Issue(
                        mechanic_id=mechanic.id,
                        date=date_str,
                        problem=problem,
                        source=str(row.get('问题来源', '')),
                        clause=str(row.get('扣分条款', '')),
                        detail=deduction,
                        total_deduction=deduction,
                        status='未结算'
                    )
                    session.add(issue)
                    
                    # Update Deduction
                    mechanic.current_cycle_deduction += deduction_mag
                    
                    # Update MonthlyRecord text (optional, but good for consistency)
                    rec = session.query(MonthlyRecord).filter_by(mechanic_id=mechanic.id, month=month).first()
                    if rec:
                        if rec.deduction is None: rec.deduction = 0.0
                        # Check if we should add? 
                        # Since MonthlyRecord wasn't cleared, it might already have deduction value.
                        # But Issue text might be missing if we rely on it?
                        # Actually MonthlyRecord stores `issues_details` string.
                        # This string is preserved.
                        # So we don't strictly need to update MonthlyRecord if we only care about Issue table and Mechanic totals.
                        pass
                    
                    count += 1
                
                print(f"  Imported {count} issues.")
                session.commit()
                
            except Exception as e:
                print(f"  Error processing {f}: {e}")
                session.rollback()
        else:
            print(f"Skipping {f} (pattern mismatch)")

    session.close()
    print("Done.")

if __name__ == "__main__":
    reimport_uploads()
