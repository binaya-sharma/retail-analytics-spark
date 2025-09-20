USE retail_gold;



CREATE TABLE IF NOT EXISTS retail_gold.dim_customer (
  customer_sk     BIGINT,
  customer_id     STRING,
  customer_name   STRING,
  gender          STRING,
  age             INT,
  email           STRING,
  phone_number    STRING,
  city            STRING,
  loyalty_tier    STRING,
  effective_from  TIMESTAMP,
  effective_to    TIMESTAMP,
  is_current      BOOLEAN,
  hashdiff        STRING
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/dim_customer';

-- Source snapshot from Silver + a stable hash of tracked attributes
CREATE OR REPLACE TEMP VIEW src_customer AS
SELECT
  customer_id,
  initcap(trim(customer_name))        AS customer_name,
  upper(trim(gender))                 AS gender,
  CAST(age AS INT)                    AS age,
  lower(trim(email))                  AS email,
  regexp_replace(phone_number,'[^0-9+]','') AS phone_number,
  initcap(trim(city))                 AS city,
  initcap(trim(loyalty_tier))         AS loyalty_tier,
  sha2(
    concat_ws('||',
      coalesce(initcap(trim(customer_name)),''),
      coalesce(upper(trim(gender)),''),
      coalesce(cast(age as string),''),
      coalesce(lower(trim(email)),''),
      coalesce(regexp_replace(phone_number,'[^0-9+]',''), ''),
      coalesce(initcap(trim(city)),''),
      coalesce(initcap(trim(loyalty_tier)),'')
    ), 256
  ) AS hashdiff,
  current_timestamp() AS as_of_ts
FROM retail_silver.customer_clean;

-- Close out changed current rows in the dimension (Type-2 close)
-- If the row exists and hashdiff changed, end the current version.
MERGE INTO retail_gold.dim_customer AS d
USING (
  SELECT s.customer_id, s.hashdiff, s.as_of_ts
  FROM src_customer s
  JOIN retail_gold.dim_customer d
    ON d.customer_id = s.customer_id AND d.is_current = true
  WHERE d.hashdiff <> s.hashdiff
) chg
ON d.customer_id = chg.customer_id AND d.is_current = true
WHEN MATCHED THEN UPDATE SET
  d.is_current     = false,
  d.effective_to   = chg.as_of_ts;

--Build the set of rows that must be inserted (brand-new OR new version)
CREATE OR REPLACE TEMP VIEW to_insert AS
SELECT
  s.customer_id,
  s.customer_name,
  s.gender,
  s.age,
  s.email,
  s.phone_number,
  s.city,
  s.loyalty_tier,
  s.hashdiff,
  s.as_of_ts      AS effective_from,
  TIMESTAMP('9999-12-31 23:59:59') AS effective_to,
  true            AS is_current
FROM src_customer s
LEFT JOIN retail_gold.dim_customer d
  ON d.customer_id = s.customer_id AND d.is_current = true
WHERE d.customer_id IS NULL              -- brand new business key
   OR d.hashdiff <> s.hashdiff;          -- new version (after close-out)

-- Assign sequential SKs to only the rows we insert in this run.
-- SK = max(existing SK) + row_number() over the batch.
WITH max_sk AS (
  SELECT COALESCE(MAX(customer_sk), 0) AS base FROM retail_gold.dim_customer
),
numbered AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id, effective_from) AS rn,
    *
  FROM to_insert
)
INSERT INTO retail_gold.dim_customer
SELECT
  (SELECT base FROM max_sk) + rn       AS customer_sk,
  customer_id,
  customer_name,
  gender,
  age,
  email,
  phone_number,
  city,
  loyalty_tier,
  effective_from,
  effective_to,
  is_current,
  hashdiff
FROM numbered;