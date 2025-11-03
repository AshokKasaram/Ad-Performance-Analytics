import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(".env")
u = os.getenv("MYSQL_USER")
p = os.getenv("MYSQL_PASSWORD")
h = os.getenv("MYSQL_HOST", "127.0.0.1")
pr = os.getenv("MYSQL_PORT", "3306")
d = os.getenv("MYSQL_DB", "meta_ads")

print(f"Connecting to {u}@{h}:{pr}/{d} ...")
try:
    engine = create_engine(f"mysql+pymysql://{u}:{p}@{h}:{pr}/{d}", future=True)
    with engine.connect() as conn:
        server_time = conn.execute(text("SELECT NOW();")).scalar()
        print("✅ Connected. Server time:", server_time)
except Exception as e:
    print("❌ Connection failed:", e)
