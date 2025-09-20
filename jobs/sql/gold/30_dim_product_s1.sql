USE retail_gold;



CREATE TABLE IF NOT EXISTS retail_gold.dim_product (
  product_sk   BIGINT,
  product_id   STRING,
  product_name STRING,
  supplier_id  STRING,
  supplier_name STRING,
  return_policy STRING,
  warranty      STRING,
  updated_at   TIMESTAMP
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/dim_product';

-- Staging view (only columns that exist in retail_silver.product_clean)
CREATE OR REPLACE TEMP VIEW src_product AS
SELECT
  trim(product_id)        AS product_id,
  initcap(trim(product_name)) AS product_name,
  trim(supplier_id)       AS supplier_id,
  initcap(trim(supplier_name)) AS supplier_name,
  return_policy,
  warranty,
  current_timestamp()     AS updated_at
FROM retail_silver.product_clean;

-- SCD1 update
MERGE INTO retail_gold.dim_product d
USING src_product s
ON d.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
  d.product_name  = s.product_name,
  d.supplier_id   = s.supplier_id,
  d.supplier_name = s.supplier_name,
  d.return_policy = s.return_policy,
  d.warranty      = s.warranty,
  d.updated_at    = s.updated_at;

-- Insert new BKs
CREATE OR REPLACE TEMP VIEW new_product AS
SELECT s.* FROM src_product s
LEFT ANTI JOIN retail_gold.dim_product d
ON d.product_id = s.product_id;

WITH base AS (SELECT COALESCE(MAX(product_sk),0) AS sk_base FROM retail_gold.dim_product),
numbered AS (SELECT ROW_NUMBER() OVER (ORDER BY product_id) rn, * FROM new_product)
INSERT INTO retail_gold.dim_product
SELECT (SELECT sk_base FROM base)+rn,
       product_id, product_name, supplier_id, supplier_name,
       return_policy, warranty, updated_at
FROM numbered;