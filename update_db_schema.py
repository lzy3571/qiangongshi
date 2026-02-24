
from database import engine, Base, Workshop, Team, User, Session

# Create new tables
Base.metadata.create_all(engine)

session = Session()

# Initialize Workshops
workshops = [
    "上海南动车所", "南翔动车所", "虹桥动车所", 
    "杭州动车所", "杭州西动车所", "宁波动车所"
]

for w_name in workshops:
    exists = session.query(Workshop).filter_by(name=w_name).first()
    if not exists:
        session.add(Workshop(name=w_name))
        print(f"Added Workshop: {w_name}")

# Check admin user
admin = session.query(User).filter_by(username='admin').first()
if admin:
    # Update admin fields if missing
    if not admin.name: admin.name = "系统管理员"
    if not admin.role: admin.role = "section"
    # Admin doesn't necessarily need a workshop, but let's leave it null
    print("Admin user updated.")

session.commit()
session.close()
print("Database schema updated and workshops initialized.")
