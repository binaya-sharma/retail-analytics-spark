-- Clean sales for Silver
CREATE OR REPLACE VIEW stg_sales AS
WITH cleaned AS (
  SELECT
    trim(sale_id)               AS sale_id,
    CAST(sale_ts AS TIMESTAMP)  AS sale_ts,
    trim(customer_id)           AS customer_id,
    trim(product_id)            AS product_id,
    trim(store_id)              AS store_id,
    trim(employee_id)           AS employee_id,
    CAST(quantity AS INT)       AS quantity,
    CAST(unit_price AS DOUBLE)  AS unit_price,
    trim(coupon_code)           AS coupon_code,
    CAST(discount_percent AS DOUBLE) AS discount_percent,
    lower(trim(mode))           AS mode,
    source_file,
    imported_date
  FROM retail_raw.sales_raw
),
filtered AS (
  SELECT *
  FROM cleaned
  WHERE sale_id IS NOT NULL
    AND quantity > 0
    AND unit_price >= 0
)
SELECT * FROM filtered;

CREATE TABLE IF NOT EXISTS retail_silver.sales_clean (
  sale_id          STRING,
  sale_ts          TIMESTAMP,
  customer_id      STRING,
  product_id       STRING,
  store_id         STRING,
  employee_id      STRING,
  quantity         INT,
  unit_price       DOUBLE,
  coupon_code      STRING,
  discount_percent DOUBLE,
  mode             STRING,
  source_file      STRING,
  imported_date    TIMESTAMP
) USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/silver/sales_clean';

-- If immutable, simple append is fine:
-- If updates may arrive, switch to MERGE on sale_id keeping newest arrival.
INSERT INTO retail_silver.sales_clean
SELECT * FROM stg_sales;

