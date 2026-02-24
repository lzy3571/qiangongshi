import sys
import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from database import Base, Mechanic, RewardHistory, Attachment6Data
from datetime import datetime

# Setup DB connection
# Assuming database.db is in the same directory
db_path = os.path.join(os.getcwd(), 'database.db')
engine = create_engine(f'sqlite:///{db_path}')
Session = sessionmaker(bind=engine)
session = Session()

def fix_data():
    print("Starting data correction...")
    
    # 1. Round activity_cumulative_hours to 2 decimals in Attachment6Data
    print("Rounding activity_cumulative_hours...")
    att6_records = session.query(Attachment6Data).all()
    for r in att6_records:
        if r.activity_cumulative_hours is not None:
            r.activity_cumulative_hours = round(r.activity_cumulative_hours, 2)
    
    # 2. Fix consecutive_info
    # It should be the 'extra_reward' of the PREVIOUS cycle.
    print("Fixing consecutive_info...")
    
    # We need to process per mechanic, ordered by reward_date/cycle
    # Get all mechanics who have history
    mechanic_ids = session.query(RewardHistory.mechanic_id).distinct().all()
    mechanic_ids = [m[0] for m in mechanic_ids]
    
    for mid in mechanic_ids:
        # Get history for this mechanic, ordered by date
        # Note: reward_date is string "YYYY-MM", reward_cycle is string "YYYY.MM.DD-..."
        # We rely on ID order or Date string sort.
        history = session.query(RewardHistory).filter_by(mechanic_id=mid).order_by(RewardHistory.reward_date.asc(), RewardHistory.id.asc()).all()
        
        for i, h in enumerate(history):
            if i == 0:
                # First record, no previous cycle
                h.consecutive_info = "0.0"
            else:
                prev = history[i-1]
                # Consecutive info is previous extra_reward
                h.consecutive_info = str(prev.extra_reward)
            
            # Also update corresponding Attachment6Data
            # We need to link RewardHistory to Attachment6Data
            # Usually they share mechanic info and reward_cycle/date
            # But Attachment6Data doesn't have a direct FK to RewardHistory in current schema
            # We match by employee_id, reward_date, reward_cycle
            
            mech = h.mechanic
            if mech:
                att6 = session.query(Attachment6Data).filter_by(
                    employee_id=mech.employee_id,
                    reward_date=h.reward_date,
                    reward_cycle=h.reward_cycle
                ).first()
                
                if att6:
                    att6.consecutive_info = h.consecutive_info
                    # Also ensure rounding here just in case
                    if att6.activity_cumulative_hours:
                        att6.activity_cumulative_hours = round(att6.activity_cumulative_hours, 2)

    session.commit()
    print("Data correction complete.")
    session.close()

if __name__ == "__main__":
    fix_data()
