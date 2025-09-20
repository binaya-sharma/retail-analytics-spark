USE retail_gold;

CREATE TABLE IF NOT EXISTS retail_gold.fact_sales (
  sale_id     STRING,
  date_sk     INT,
  customer_sk BIGINT,
  employee_sk STRING,
  product_sk  BIGINT,
  store_sk    BIGINT,
  quantity    INT,
  unit_price  DECIMAL(18,2),
  discount_amt DECIMAL(18,2),
  gross_amt    DECIMAL(18,2),
  net_amt      DECIMAL(18,2),
  load_ts     TIMESTAMP
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/fact_sales';

-- Build the source rows (joins only on columns that exist)
CREATE OR REPLACE TEMP VIEW fact_src AS
SELECT
  CAST (s.sale_id AS STRING )                        AS sale_id,
  d.date_sk                                        AS date_sk,
  c.customer_sk                                    AS customer_sk,
  e.employee_sk                                    AS employee_sk,
  p.product_sk                                     AS product_sk,
  st.store_sk                                      AS store_sk,
  CAST(s.quantity AS INT)                          AS quantity,
  CAST(s.unit_price AS DECIMAL(18,2))              AS unit_price,
  /* derive $ if not already present in silver */
  CAST((CAST(s.quantity AS DECIMAL(10,0)) * s.unit_price
        * (COALESCE(s.discount_percent, 0) / 100)) AS DECIMAL(18,2)) AS discount_amt,
  CAST((CAST(s.quantity AS DECIMAL(10,0)) * s.unit_price) AS DECIMAL(18,2))           AS gross_amt,
  CAST((CAST(s.quantity AS DECIMAL(10,0)) * s.unit_price)
       - (CAST(s.quantity AS DECIMAL(10,0)) * s.unit_price
          * (COALESCE(s.discount_percent, 0) / 100)) AS DECIMAL(18,2))                AS net_amt,
  current_timestamp()                              AS load_ts
FROM retail_silver.sales_clean s
LEFT JOIN retail_gold.dim_date     d  ON d.full_date = CAST(s.sale_ts AS DATE)
LEFT JOIN retail_gold.dim_product  p  ON p.product_id = s.product_id
LEFT JOIN retail_gold.dim_store    st ON st.store_id = s.store_id
LEFT JOIN retail_gold.dim_customer c  ON c.customer_id = s.customer_id AND c.is_current = true
LEFT JOIN retail_gold.dim_employee e  ON e.employee_id = s.employee_id AND e.is_current = true;

-- Simple insert with a few safe conditions (no 'active')
INSERT INTO retail_gold.fact_sales
SELECT
  sale_id,
  date_sk,
  customer_sk,
  employee_sk,
  product_sk,
  store_sk,
  quantity,
  unit_price,
  COALESCE(discount_amt, 0) AS discount_amt,
  COALESCE(gross_amt, CAST(quantity AS DECIMAL(18,2)) * unit_price) AS gross_amt,
  COALESCE(net_amt,
           COALESCE(gross_amt, CAST(quantity AS DECIMAL(18,2)) * unit_price)
           - COALESCE(discount_amt, 0)) AS net_amt,
  load_ts
FROM fact_src
WHERE sale_id IS NOT NULL
  AND quantity IS NOT NULL AND unit_price IS NOT NULL
  AND quantity >= 0 AND unit_price >= 0;