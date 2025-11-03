#!/usr/bin/env python3
"""
ETL: CSV -> MySQL (fact/dim model) for Meta Ad Analytics
Requires:
  pip install pandas numpy SQLAlchemy PyMySQL python-dotenv
Usage:
  1) CREATE DATABASE meta_ads;
  2) Copy .env.example to .env and fill creds
  3) python etl_mysql.py
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(__file__) if "__file__" in globals() else "."
RAW_CSV = os.path.join(BASE_DIR, "meta_ads_raw.csv")
SCHEMA_SQL = os.path.join(BASE_DIR, "schema_mysql.sql")

load_dotenv(os.path.join(BASE_DIR, ".env"))

DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = os.getenv("MYSQL_PORT", "3306")
DB_NAME = os.getenv("MYSQL_DB", "meta_ads")

ENGINE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def ensure_schema(engine):
    with engine.connect() as conn:
        with open(SCHEMA_SQL, "r") as f:
            sql = f.read()
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt + ";"))
        conn.commit()

def build_dim_campaign(engine):
    import pandas as pd
    campaigns = pd.DataFrame([
        (1001, "Onboarding Awareness", "AWARENESS"),
        (1002, "Signup Conversion", "CONVERSIONS"),
        (1003, "Mobile App Installs", "APP_INSTALLS"),
        (1004, "Re-Engagement Push", "RE_ENGAGEMENT"),
        (1005, "Holiday Promo", "SALES"),
        (1006, "Brand Lift Test", "BRAND"),
        (1007, "New Feature Launch", "AWARENESS"),
        (1008, "Retention LTV", "RETENTION"),
    ], columns=["campaign_id","campaign_name","objective"])
    campaigns.to_sql("dim_campaign", engine, if_exists="append", index=False)

def build_dim_date(engine, df):
    dd = (df[["date"]].drop_duplicates().rename(columns={"date":"date_key"}))
    dd["date_key"] = pd.to_datetime(dd["date_key"]).dt.date
    dd["year"] = pd.to_datetime(dd["date_key"]).dt.year
    dd["month"] = pd.to_datetime(dd["date_key"]).dt.month
    dd["day"] = pd.to_datetime(dd["date_key"]).dt.day
    dd["week"] = pd.to_datetime(dd["date_key"]).dt.isocalendar().week.astype(int)
    dd.to_sql("dim_date", engine, if_exists="append", index=False)

def transform(df):
    df = df.copy()
    df["ctr"] = (df["clicks"] / df["impressions"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["cpc"] = (df["spend"] / df["clicks"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["cpm"] = (df["spend"] / df["impressions"] * 1000.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["roi"] = ((df["revenue"] - df["spend"]) / df["spend"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df = df.rename(columns={"date": "date_key"})
    df["date_key"] = pd.to_datetime(df["date_key"]).dt.date
    cols = ["ad_id","campaign_id","date_key","impressions","clicks","spend","conversions","revenue","ctr","cpc","cpm","roi"]
    return df[cols]

def main():
    print("Reading:", RAW_CSV)
    df = pd.read_csv(RAW_CSV)
    engine = create_engine(ENGINE_URL, future=True)
    print("Ensuring schema...")
    ensure_schema(engine)
    print("Loading dims...")
    build_dim_campaign(engine)
    build_dim_date(engine, df)
    print("Transforming + loading fact...")
    fact = transform(df)
    fact.to_sql("fact_ads", engine, if_exists="append", index=False)
    print("Done. Query vw_campaign_agg & vw_campaign_daily in MySQL.")

if __name__ == "__main__":
    main()