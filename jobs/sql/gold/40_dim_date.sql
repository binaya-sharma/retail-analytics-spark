USE retail_gold;



CREATE TABLE IF NOT EXISTS retail_gold.dim_date (
  date_sk     INT,
  full_date   DATE,
  day         INT,
  month       INT,
  year        INT,
  quarter     INT,
  day_name    STRING,
  month_name  STRING,
  is_weekend  BOOLEAN
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/dim_date';

-- Generate range of dates (e.g., 2015–2035)
WITH seq AS (
  SELECT explode(sequence(
      to_date('2015-01-01'),
      to_date('2035-12-31'),
      interval 1 day
  )) AS full_date
)
INSERT OVERWRITE retail_gold.dim_date
SELECT
  CAST(date_format(full_date,'yyyyMMdd') AS INT) AS date_sk,
  full_date,
  day(full_date)    AS day,
  month(full_date)  AS month,
  year(full_date)   AS year,
  quarter(full_date) AS quarter,
  date_format(full_date,'EEEE')   AS day_name,
  date_format(full_date,'MMMM')   AS month_name,
  CASE WHEN dayofweek(full_date) IN (1,7) THEN true ELSE false END AS is_weekend
FROM seq;