from database import Session, User, engine, Base
import sys

def init_user():
    # Create table if not exists
    Base.metadata.create_all(engine)
    
    session = Session()
    # Check if admin exists
    admin = session.query(User).filter_by(username='admin').first()
    if not admin:
        print("Creating default admin user...")
        admin = User(username='admin', password='123', role='section')
        session.add(admin)
        session.commit()
        print("Admin user created (admin/123).")
    else:
        print("Admin user already exists.")
    session.close()

if __name__ == "__main__":
    init_user()
