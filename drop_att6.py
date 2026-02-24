
from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS attachment6_data"))
    print("Dropped attachment6_data table.")
