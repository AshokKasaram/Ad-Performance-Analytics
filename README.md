# Marketing-Performance-Analytics
This project demonstrates the complete data analytics lifecycle — from data extraction and transformation (ETL) to SQL modeling and Power BI dashboard visualization, turning raw data into actionable business insights.

### Project Goal
Design and build an **end-to-end data pipeline** to simulate Meta Ads campaign performance, calculate key marketing KPIs (CTR, CPC, CPM, ROI), and visualize insights through an interactive **Power BI dashboard**.

This project demonstrates the **full lifecycle of data engineering and analytics** — from raw data extraction and transformation (ETL) to SQL modeling and dashboard storytelling.

---

## Architecture Overview
    Raw CSV (Ad Campaign Data)
    ↓
    Python ETL (pandas, SQLAlchemy)
    ↓
    MySQL Database (fact & dimension tables)
    ↓
    Power BI Dashboard (KPIs, CTR Trend, ROI Analysis)

---

## Tech Stack
- **Python** → Data extraction & transformation (`pandas`, `SQLAlchemy`, `PyMySQL`, `dotenv`)
- **MySQL** → Data warehouse & SQL modeling
- **Power BI** → Visualization and insight delivery
- **VS Code** → Development & automation

---

## ETL Pipeline Design

Your ETL pipeline automates the flow of data from raw input → clean database → analytics-ready format.

### ** Extract**
Source: Simulated Meta Ads dataset **`meta_ads_raw.csv`**

Example fields:
    ad_id, campaign_id, impressions, clicks, spend, revenue, date

 **Code Snippet**
    import pandas as pd
    df = pd.read_csv("meta_ads_raw.csv")

This simulates data ingestion from APIs such as Meta Marketing API or Google Ads API.

---

### ** Transform**

Clean, standardize, and enrich the data using **pandas**, computing KPIs:

| KPI         | Formula                             | Description                |
| ----------- | ----------------------------------- | -------------------------- |
| **CTR (%)** | `(clicks / impressions) * 100`      | Engagement metric          |
| **CPC ($)** | `spend / clicks`                    | Cost per click             |
| **CPM ($)** | `(spend / impressions) * 1000`      | Cost per 1,000 impressions |
| **ROI (%)** | `((revenue - spend) / spend) * 100` | Return on investment       |

 **Transformation Snippet**
    df["ctr_pct"] = round(df["clicks"] * 100 / df["impressions"], 2)
    df["cpc"] = round(df["spend"] / df["clicks"], 2)
    df["cpm"] = round(df["spend"] * 1000 / df["impressions"], 2)
    df["roi"] = round((df["revenue"] - df["spend"]) * 100 / df["spend"], 2)

---

### ** Load**

Store the transformed dataset into a **MySQL** database using `SQLAlchemy` for structured analytics.

**Tables**
- `dim_campaign` — campaign metadata  
- `dim_date` — calendar dimension  
- `fact_ads` — all ad performance records  

 **Load Snippet**
    from sqlalchemy import create_engine
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    df.to_sql("fact_ads", con=engine, if_exists="replace", index=False)

---

### ** SQL Modeling**

Two analytical SQL views were created to aggregate and expose KPIs.

    CREATE VIEW vw_campaign_agg AS
    SELECT campaign_id,
           SUM(impressions) AS impressions,
           SUM(clicks) AS clicks,
           SUM(spend) AS spend,
           SUM(revenue) AS revenue,
           ROUND(SUM(clicks)*100.0/SUM(impressions), 2) AS ctr_pct,
           ROUND(SUM(spend)/NULLIF(SUM(clicks),0), 2) AS cpc,
           ROUND(SUM(spend)*1000/NULLIF(SUM(impressions),0), 2) AS cpm,
           ROUND((SUM(revenue)-SUM(spend))*100.0/NULLIF(SUM(spend),0), 2) AS roi
    FROM fact_ads
    GROUP BY campaign_id;

 **Views Created**
- `vw_campaign_agg` — Campaign-level summary  
- `vw_campaign_daily` — Daily trends for CTR and ROI  

---

##  Power BI Dashboard

### **Dashboard Highlights**
Interactive dashboard built in **Power BI** displaying KPIs and trends for Meta-style ad campaigns.

####  Components

| Section          | Visualization                  | Data Source         |
| ---------------- | ------------------------------ | ------------------- |
| KPI Cards        | Spend, Revenue, CTR, CPC, ROI  | `vw_campaign_agg`   |
| CTR Trend        | Line Chart by Date             | `vw_campaign_daily` |
| Spend vs Revenue | Clustered Column Chart         | `vw_campaign_agg`   |
| Campaign Details | ROI, CTR, CPC, CPM table       | `vw_campaign_agg`   |
| Filters          | Date, Campaign Name, Objective | Both views          |

![Dashboard Preview](Ad Performance Analytics Dashboard Preview.jpg)

---

## Key Insights
- **Signup Conversion** campaign achieved the highest ROI (+29%)  
- **Brand Lift Test** and **Awareness** campaigns underperformed (negative ROI)  
- CTR peaked mid-October, revealing an engagement sweet spot  
- Spend and ROI correlation shows where budget efficiency can improve  

---

## KPIs Tracked

| Metric               | Meaning                     |
| -------------------- | --------------------------- |
| **CTR (%)**          | Click-Through Rate          |
| **CPC ($)**          | Cost per Click              |
| **CPM ($)**          | Cost per 1000 Impressions   |
| **ROI (%)**          | Return on Investment        |
| **Spend vs Revenue** | Marketing budget efficiency |

---

## Learnings
- Designed and executed a **modular ETL pipeline** in Python  
- Modeled marketing data in **MySQL** using fact/dimension schema  
- Built a professional **Power BI dashboard** for KPI visualization  
- Practiced **data storytelling** and marketing performance analytics  

---

## Future Improvements
- Integrate **Meta Ads API** for real-time ingestion  
- Schedule ETL with **Airflow / Prefect**  
- Add **predictive models** for ROI forecasting  
- Deploy as a web app via **Streamlit / Flask / Power BI Service**  

---

## Repository Structure
    meta-ad-analytics-pipeline/
    │
    ├── etl_mysql.py
    ├── schema_mysql.sql
    ├── meta_ads_raw.csv
    ├── .env.example
    ├── requirements.txt
    ├── Meta_Ad_Analytics_Dashboard.pbix
    ├── dashboard_preview.png
    └── README.md

---

## Author
**Ashok Kasaram**  
 *M.S. in Data Science & Artificial Intelligence, Florida International University*  
 [ashok.kasaram99@gmail.com](mailto:ashok.kasaram99@gmail.com)  
 [LinkedIn](https://www.linkedin.com/in/ashokkasaram) | [GitHub](https://github.com/ashokkasaram)

---

*If you found this project insightful, please give it a star on GitHub!*

