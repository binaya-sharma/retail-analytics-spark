-- Stage: clean + dedupe latest per customer_id
CREATE OR REPLACE VIEW stg_customer AS
WITH cleaned AS (
  SELECT
    trim(customer_id)                           AS customer_id,
    initcap(trim(customer_name))                AS customer_name,
    upper(trim(gender))                         AS gender,
    CAST(age AS INT)                            AS age,
    lower(trim(email))                          AS email,
    regexp_replace(phone_number, '[^0-9+]', '') AS phone_number,
    initcap(trim(city))                         AS city,
    initcap(trim(loyalty_tier))                 AS loyalty_tier,
    source_file,
    imported_date
  FROM retail_raw.customer
),
dedup AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY customer_id
           ORDER BY imported_date DESC, source_file DESC
         ) AS rn
  FROM cleaned
)
SELECT * FROM dedup WHERE rn = 1 AND customer_id IS NOT NULL;

-- Silver table
CREATE TABLE IF NOT EXISTS retail_silver.customer_clean (
  customer_id   STRING,
  customer_name STRING,
  gender        STRING,
  age           INT,
  email         STRING,
  phone_number  STRING,
  city          STRING,
  loyalty_tier  STRING,
  source_file   STRING,
  imported_date TIMESTAMP
) USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/silver/customer_clean';

-- Upsert into Silver
MERGE INTO retail_silver.customer_clean AS tgt
USING  stg_customer                     AS src
ON     tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;