from app import load_config, build_spark

def run_sql_file(spark, path):
    with open(path, "r") as f:
        sql_text = f.read()
    for stmt in [s for s in sql_text.split(";") if s.strip()]:
        spark.sql(stmt)

if __name__ == "__main__":
    cfg = load_config()
    spark = build_spark(cfg)

    base = "jobs/sql/silver"
    for file in [
        f"{base}/_session.sql",
        f"{base}/customer.sql",
        f"{base}/product.sql",
        f"{base}/store.sql",
        f"{base}/supplier.sql",
        f"{base}/employee_history.sql",
        f"{base}/sales.sql",
    ]:
        run_sql_file(spark, file)

    print("Silver layer build complete.")