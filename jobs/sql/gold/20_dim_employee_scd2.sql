USE retail_gold;

CREATE TABLE IF NOT EXISTS retail_gold.dim_employee (
  employee_sk    STRING,             -- UUID
  employee_id    STRING,
  employee_code  STRING,
  store_id       STRING,
  store_name     STRING,
  effective_from TIMESTAMP,
  effective_to   TIMESTAMP,
  is_current     BOOLEAN,
  is_primary     BOOLEAN,
  source_file    STRING,
  imported_date  TIMESTAMP
)
USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/gold/dim_employee';

-- latest cleaned rows from silver
CREATE OR REPLACE VIEW stg_employee AS
SELECT
  trim(employee_id)   AS employee_id,
  trim(employee_code) AS employee_code,
  trim(store_id)      AS store_id,
  initcap(trim(store_name)) AS store_name,
  source_file,
  imported_date
FROM retail_silver.employee_clean_history;

-- find changes vs current open rows
CREATE OR REPLACE VIEW emp_changes AS
WITH current_open AS (
  SELECT *
  FROM retail_gold.dim_employee
  WHERE is_current = true
),
incoming_ranked AS (
  SELECT e.*,
         row_number() OVER (PARTITION BY employee_id ORDER BY imported_date DESC) AS rn
  FROM stg_employee e
)
SELECT i.*
FROM incoming_ranked i
LEFT JOIN current_open c
  ON c.employee_id = i.employee_id
WHERE i.rn = 1 AND (
     c.employee_id IS NULL
  OR c.employee_code <> i.employee_code
  OR c.store_id      <> i.store_id
);

-- close existing (if changed)
MERGE INTO retail_gold.dim_employee tgt
USING emp_changes src
ON tgt.employee_id = src.employee_id AND tgt.is_current = true
WHEN MATCHED THEN UPDATE SET
  tgt.effective_to = current_timestamp(),
  tgt.is_current   = false;

-- insert new version (UUID key)
INSERT INTO retail_gold.dim_employee (
  employee_sk, employee_id, employee_code, store_id, store_name,
  effective_from, effective_to, is_current, is_primary,
  source_file, imported_date
)
SELECT
  uuid(), employee_id, employee_code, store_id, store_name,
  current_timestamp(), TIMESTAMP'9999-12-31', true, true,
  source_file, imported_date
FROM emp_changes;