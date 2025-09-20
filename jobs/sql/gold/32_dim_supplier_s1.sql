USE retail_gold;



-- 1) Create the dimension table (only columns available from silver)
CREATE TABLE IF NOT EXISTS retail_gold.dim_supplier (
  supplier_sk   BIGINT,
  supplier_id   STRING,
  supplier_name STRING,
  updated_at    TIMESTAMP
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/dim_supplier';

-- 2) Source rows coming from silver
CREATE OR REPLACE TEMP VIEW src_supplier AS
SELECT
  trim(supplier_id)            AS supplier_id,
  initcap(trim(supplier_name)) AS supplier_name,
  current_timestamp()          AS updated_at
FROM retail_silver.supplier_clean;

-- 3) Upsert existing keys (by business key)
MERGE INTO retail_gold.dim_supplier d
USING src_supplier s
ON d.supplier_id = s.supplier_id
WHEN MATCHED THEN UPDATE SET
  d.supplier_name = s.supplier_name,
  d.updated_at    = s.updated_at;

-- 4) Insert brand-new suppliers with generated surrogate keys
WITH base AS (
  SELECT COALESCE(MAX(supplier_sk), 0) AS sk_base FROM retail_gold.dim_supplier
),
new_supplier AS (
  SELECT s.*
  FROM src_supplier s
  LEFT ANTI JOIN retail_gold.dim_supplier d
    ON d.supplier_id = s.supplier_id
),
numbered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY supplier_id) AS rn, *
  FROM new_supplier
)
INSERT INTO retail_gold.dim_supplier
SELECT b.sk_base + n.rn     AS supplier_sk,
       n.supplier_id,
       n.supplier_name,
       n.updated_at
FROM numbered n
CROSS JOIN base b;