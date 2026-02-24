
import sqlite3

def add_columns():
    conn = sqlite3.connect('mechanics.db')
    c = conn.cursor()
    
    # Check if columns exist, if not add them
    try:
        c.execute("ALTER TABLE users ADD COLUMN employee_id TEXT")
        print("Added employee_id")
    except:
        pass
        
    try:
        c.execute("ALTER TABLE users ADD COLUMN name TEXT")
        print("Added name")
    except:
        pass
        
    try:
        c.execute("ALTER TABLE users ADD COLUMN contact TEXT")
        print("Added contact")
    except:
        pass
        
    try:
        c.execute("ALTER TABLE users ADD COLUMN workshop_id INTEGER REFERENCES workshops(id)")
        print("Added workshop_id")
    except:
        pass
        
    conn.commit()
    conn.close()

if __name__ == '__main__':
    add_columns()
