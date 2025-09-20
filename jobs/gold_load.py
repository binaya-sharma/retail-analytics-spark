from app import load_config, build_spark
import pathlib

def run_sql_file(spark, path):
    sql = pathlib.Path(path).read_text()
    # split on ; but keep it simple: ignore semicolons inside strings (not common in these scripts)
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        spark.sql(stmt)

if __name__ == "__main__":
    cfg = load_config()
    spark = build_spark(cfg)

    base = "file:/Users/Binaya/Documents/spark1"
    local_root = "jobs/sql/gold"  # relative to project

    files = [
        f"{local_root}/_session.sql",
        f"{local_root}/10_dim_customer_scd2.sql",
        f"{local_root}/20_dim_employee_scd2.sql",
        f"{local_root}/30_dim_product_s1.sql",
        f"{local_root}/31_dim_store_s1.sql",
        f"{local_root}/32_dim_supplier_s1.sql",
        f"{local_root}/40_dim_date.sql",
        f"{local_root}/50_fact_sales.sql",
    ]
    for f in files:
        print(f"Running {f}")
        run_sql_file(spark, f)
    print("Gold load complete.")