CREATE OR REPLACE VIEW stg_store AS
WITH cleaned AS (
  SELECT
    trim(store_id)                AS store_id,
    initcap(trim(store_name))     AS store_name,
    initcap(trim(city))           AS city,
    source_file,
    imported_date
  FROM retail_raw.store
),
dedup AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY store_id
           ORDER BY imported_date DESC, source_file DESC
         ) AS rn
  FROM cleaned
)
SELECT * FROM dedup WHERE rn = 1 AND store_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS retail_silver.store_clean (
  store_id     STRING,
  store_name   STRING,
  city         STRING,
  source_file  STRING,
  imported_date TIMESTAMP
) USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/silver/store_clean';

MERGE INTO retail_silver.store_clean AS tgt
USING  stg_store                     AS src
ON     tgt.store_id = src.store_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;