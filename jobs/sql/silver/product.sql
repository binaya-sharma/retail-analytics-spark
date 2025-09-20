CREATE OR REPLACE VIEW stg_product AS
WITH cleaned AS (
  SELECT
    trim(product_id)                   AS product_id,
    initcap(trim(product_name))        AS product_name,
    trim(supplier_id)                  AS supplier_id,
    initcap(trim(supplier_name))       AS supplier_name,
    trim(return_policy)                AS return_policy,
    trim(warranty)                     AS warranty,
    source_file,
    imported_date
  FROM retail_raw.product
),
dedup AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY product_id
           ORDER BY imported_date DESC, source_file DESC
         ) AS rn
  FROM cleaned
)
SELECT * FROM dedup WHERE rn = 1 AND product_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS retail_silver.product_clean (
  product_id    STRING,
  product_name  STRING,
  supplier_id   STRING,
  supplier_name STRING,
  return_policy STRING,
  warranty      STRING,
  source_file   STRING,
  imported_date TIMESTAMP
) USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/silver/product_clean';

MERGE INTO retail_silver.product_clean AS tgt
USING  stg_product                     AS src
ON     tgt.product_id = src.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;