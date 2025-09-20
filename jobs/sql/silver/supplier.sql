CREATE OR REPLACE VIEW stg_supplier AS
WITH cleaned AS (
  SELECT
    trim(supplier_id)            AS supplier_id,
    initcap(trim(supplier_name)) AS supplier_name,
    source_file,
    imported_date
  FROM retail_raw.supplier
),
dedup AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY supplier_id
           ORDER BY imported_date DESC, source_file DESC
         ) AS rn
  FROM cleaned
)
SELECT * FROM dedup WHERE rn = 1 AND supplier_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS retail_silver.supplier_clean (
  supplier_id   STRING,
  supplier_name STRING,
  source_file   STRING,
  imported_date TIMESTAMP
) USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/silver/supplier_clean';

MERGE INTO retail_silver.supplier_clean AS tgt
USING  stg_supplier                       AS src
ON     tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;