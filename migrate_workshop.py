from database import engine, Session, Workshop, Mechanic, User
from sqlalchemy import text

session = Session()

# 1. Add column if not exists
try:
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE mechanics ADD COLUMN workshop_id INTEGER REFERENCES workshops(id)"))
        conn.commit()
    print("Added workshop_id column to mechanics table.")
except Exception as e:
    print(f"Column might already exist: {e}")

# 2. Ensure Workshop exists
sh_workshop = session.query(Workshop).filter_by(name='上海南动车所').first()
if not sh_workshop:
    sh_workshop = Workshop(name='上海南动车所')
    session.add(sh_workshop)
    session.commit()
    print("Created Workshop: 上海南动车所")
else:
    print(f"Found Workshop: {sh_workshop.name} (ID: {sh_workshop.id})")

# 3. Update Mechanics
mechanics = session.query(Mechanic).filter(Mechanic.workshop_id == None).all()
for m in mechanics:
    m.workshop_id = sh_workshop.id
session.commit()
print(f"Updated {len(mechanics)} mechanics to workshop {sh_workshop.name}")

session.close()
