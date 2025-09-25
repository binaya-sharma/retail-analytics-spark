-- Normalize + prepare change_hash
CREATE OR REPLACE VIEW stg_employee_hist AS
SELECT
  trim(employee_id) AS employee_id,
  trim(employee_code)  AS employee_code,
  trim(store_id)  AS store_id,
  initcap(trim(store_name)) AS store_name,
  CAST(effective_from AS DATE) AS effective_from,
  COALESCE(CAST(effective_to AS DATE), DATE '9999-12-31') AS effective_to,
  CASE WHEN lower(CAST(is_current AS STRING)) IN ('1','true','t','y','yes') THEN true ELSE false END AS is_current,
  CASE WHEN lower(CAST(is_primary AS STRING)) IN ('1','true','t','y','yes') THEN true ELSE false END AS is_primary,
  sha2(
    coalesce(employee_code,'') || '|' ||
    coalesce(store_id,'')      || '|' ||
    coalesce(store_name,'')    || '|' ||
    coalesce(string(is_primary),'')
  , 256) AS change_hash,
  source_file,
  imported_date
FROM retail_raw.employee
WHERE employee_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS retail_silver.employee_clean_history (
  employee_id    STRING,
  employee_code  STRING,
  store_id       STRING,
  store_name     STRING,
  effective_from DATE,
  effective_to   DATE,
  is_current     BOOLEAN,
  is_primary     BOOLEAN,
  change_hash    STRING,
  source_file    STRING,
  imported_date  TIMESTAMP
) USING DELTA
LOCATION 'file:/Users/Binaya/Documents/spark1/data/silver/employee_clean_history';

--close overlapping current rows on change
MERGE INTO retail_silver.employee_clean_history AS tgt
USING stg_employee_hist AS src
ON  tgt.employee_id = src.employee_id
AND tgt.is_current = true
AND src.effective_from BETWEEN tgt.effective_from AND COALESCE(tgt.effective_to, DATE '9999-12-31')
AND tgt.change_hash <> src.change_hash
WHEN MATCHED THEN UPDATE SET
  tgt.effective_to = DATEADD(day, -1, src.effective_from),
  tgt.is_current   = false,
  tgt.imported_date = GREATEST(tgt.imported_date, src.imported_date);

--insert new versions 
MERGE INTO retail_silver.employee_clean_history AS tgt
USING stg_employee_hist AS src
ON  tgt.employee_id  = src.employee_id
AND tgt.effective_from = src.effective_from
AND tgt.change_hash = src.change_hash
WHEN NOT MATCHED THEN INSERT (
  employee_id, employee_code, store_id, store_name,
  effective_from, effective_to, is_current, is_primary,
  change_hash, source_file, imported_date
)
VALUES (
  src.employee_id, src.employee_code, src.store_id, src.store_name,
  src.effective_from, src.effective_to,
  /* new records become current :: the previous step already closed any overlapping row */
  true, src.is_primary,
  src.change_hash, src.source_file, src.imported_date
);
