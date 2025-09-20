USE retail_gold;


CREATE TABLE IF NOT EXISTS retail_gold.dim_store (
  store_sk    BIGINT,
  store_id    STRING,
  store_name  STRING,
  city        STRING,
  updated_at  TIMESTAMP
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/dim_store';

-- Staging view (only columns available in retail_silver.store_clean)
CREATE OR REPLACE TEMP VIEW src_store AS
SELECT
  trim(store_id)        AS store_id,
  initcap(trim(store_name)) AS store_name,
  initcap(trim(city))   AS city,
  current_timestamp()   AS updated_at
FROM retail_silver.store_clean;

-- SCD1 update
MERGE INTO retail_gold.dim_store d
USING src_store s
ON d.store_id = s.store_id
WHEN MATCHED THEN UPDATE SET
  d.store_name = s.store_name,
  d.city       = s.city,
  d.updated_at = s.updated_at;

-- Insert new BKs
CREATE OR REPLACE TEMP VIEW new_store AS
SELECT s.* FROM src_store s
LEFT ANTI JOIN retail_gold.dim_store d
ON d.store_id = s.store_id;

WITH base AS (SELECT COALESCE(MAX(store_sk),0) AS sk_base FROM retail_gold.dim_store),
numbered AS (SELECT ROW_NUMBER() OVER (ORDER BY store_id) rn, * FROM new_store)
INSERT INTO retail_gold.dim_store
SELECT (SELECT sk_base FROM base)+rn,
       store_id, store_name, city, updated_at
FROM numbered;