-- MySQL schema for Meta Ad Analytics (run in MySQL Workbench)

DROP VIEW IF EXISTS vw_campaign_daily;
DROP VIEW IF EXISTS vw_campaign_agg;

DROP TABLE IF EXISTS fact_ads;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_campaign;

CREATE TABLE dim_campaign (
  campaign_id INT PRIMARY KEY,
  campaign_name VARCHAR(100),
  objective VARCHAR(50)
);

CREATE TABLE dim_date (
  date_key DATE PRIMARY KEY,
  year INT,
  month INT,
  day INT,
  week INT
);

CREATE TABLE fact_ads (
  ad_id VARCHAR(32),
  campaign_id INT,
  date_key DATE,
  impressions INT,
  clicks INT,
  spend DECIMAL(12,2),
  conversions INT,
  revenue DECIMAL(12,2),
  ctr DECIMAL(10,6),
  cpc DECIMAL(10,6),
  cpm DECIMAL(10,6),
  roi DECIMAL(12,6),
  FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id),
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  INDEX idx_fact_campaign_date (campaign_id, date_key),
  INDEX idx_fact_date (date_key)
);

-- DAILY view
CREATE VIEW vw_campaign_daily AS
SELECT
  f.date_key,
  f.campaign_id,
  d.year, d.month, d.week,
  dc.campaign_name,
  dc.objective,
  SUM(f.impressions) AS impressions,
  SUM(f.clicks) AS clicks,
  ROUND(SUM(f.spend), 2) AS spend,
  SUM(f.conversions) AS conversions,
  ROUND(SUM(f.revenue), 2) AS revenue,
  ROUND(100.0 * SUM(f.clicks) / NULLIF(SUM(f.impressions), 0), 2) AS ctr_pct,
  ROUND(SUM(f.spend) / NULLIF(SUM(f.clicks), 0), 4) AS cpc,
  ROUND(1000.0 * SUM(f.spend) / NULLIF(SUM(f.impressions), 0), 4) AS cpm,
  ROUND((SUM(f.revenue) - SUM(f.spend)) / NULLIF(SUM(f.spend), 0), 4) AS roi
FROM fact_ads f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_campaign dc ON dc.campaign_id = f.campaign_id
GROUP BY f.date_key, f.campaign_id, d.year, d.month, d.week, dc.campaign_name, dc.objective;

-- AGGREGATE view
CREATE VIEW vw_campaign_agg AS
SELECT
  f.campaign_id,
  dc.campaign_name,
  dc.objective,
  SUM(f.impressions) AS impressions,
  SUM(f.clicks) AS clicks,
  ROUND(SUM(f.spend), 2) AS spend,
  SUM(f.conversions) AS conversions,
  ROUND(SUM(f.revenue), 2) AS revenue,
  ROUND(100.0 * SUM(f.clicks) / NULLIF(SUM(f.impressions), 0), 2) AS ctr_pct,
  ROUND(SUM(f.spend) / NULLIF(SUM(f.clicks), 0), 4) AS cpc,
  ROUND(1000.0 * SUM(f.spend) / NULLIF(SUM(f.impressions), 0), 4) AS cpm,
  ROUND((SUM(f.revenue) - SUM(f.spend)) / NULLIF(SUM(f.spend), 0), 4) AS roi
FROM fact_ads f
JOIN dim_campaign dc ON dc.campaign_id = f.campaign_id
GROUP BY f.campaign_id, dc.campaign_name, dc.objective
ORDER BY ctr_pct DESC;