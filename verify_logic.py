from database import Session, Mechanic, MonthlyRecord, init_db
from sqlalchemy import func

def verify():
    init_db()
    session = Session()
    
    # Clean up test data
    session.query(MonthlyRecord).filter(MonthlyRecord.month.like('TEST%')).delete()
    session.query(Mechanic).filter_by(employee_id='TEST001').delete()
    session.commit()
    
    # 1. Create Mechanic with Base Hours (Att 2)
    m = Mechanic(employee_id='TEST001', name='TestUser', total_hours=500.0)
    session.add(m)
    session.commit()
    print(f"Initial: {m.total_hours}") # Expect 500
    
    # 2. Simulate Import New (Jan)
    month = 'TEST-01'
    hours = 100.0
    
    existing = session.query(MonthlyRecord).filter_by(mechanic_id=m.id, month=month).first()
    if existing:
        m.total_hours -= existing.hours
        existing.hours = hours
        m.total_hours += hours
    else:
        m.total_hours += hours
        rec = MonthlyRecord(mechanic=m, month=month, hours=hours)
        session.add(rec)
    session.commit()
    print(f"After Import Jan (100): {m.total_hours}") # Expect 600
    
    # 3. Simulate Import Update (Jan -> 110)
    hours = 110.0
    existing = session.query(MonthlyRecord).filter_by(mechanic_id=m.id, month=month).first()
    if existing:
        m.total_hours -= existing.hours
        existing.hours = hours
        m.total_hours += hours
    session.commit()
    print(f"After Update Jan (110): {m.total_hours}") # Expect 610
    
    # 4. Simulate Init Data (Reset)
    # Init data reads Excel and sets total_hours directly
    m.total_hours = 500.0
    session.commit()
    print(f"After Init Data Reset: {m.total_hours}") # Expect 500 (Data Loss!)
    
    # 5. Proposed Fix for Init Data
    # After setting base, add sum of monthly records
    monthly_sum = session.query(func.sum(MonthlyRecord.hours)).filter_by(mechanic_id=m.id).scalar() or 0.0
    m.total_hours += monthly_sum
    session.commit()
    print(f"After Fixed Init Data: {m.total_hours}") # Expect 610
    
    session.close()

if __name__ == "__main__":
    verify()
