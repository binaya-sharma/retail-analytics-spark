# src/jobs/bronze_load.py
from app import load_config, build_spark
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name, current_timestamp, regexp_extract
from utils.logger import get_logger

# ─────────────────────────────────────────────────────────────
# Column Ordering in Spark Structured Streaming + Delta Lake
# 
# - DataFrame API:
#   * withColumn() appends new fields (e.g., source_file, imported_date) 
#     at the end of the logical schema.
#   * If upstream schema evolves (new fields like mvp_yn), they appear 
#     before the derived columns, but ordering does not affect semantics.
#
# - Delta Lake:
#   * Column alignment during writes is NAME-BASED, not POSITION-BASED.
#   * Schema evolution (mergeSchema + autoMerge.enabled) ensures new
#     fields are merged safely, with NULL backfill for historical rows.
#
# - Caveat with SQL Insert:
#   * SQL "INSERT INTO table SELECT ..." without explicit column list
#     is position-based → avoid in production.
#
# - input_file_name() works in both batch (read) and streaming (readStream) modes in Spark.
# ─────────────────────────────────────────────────────────────

logger = get_logger('bronze_layer_log')

def log(message):
    if logger:
        logger.info(message)
    else:
        print(message)

def ingest_entity(spark, name, input_path, table_location, checkpoint_path, schema):
    print(f"=== Ingesting {name} into {table_location} ===")

    df_stream = (
        spark.readStream
             .format("csv")
             .option("header", "true")
             .schema(schema)
             .load(input_path)
             .withColumn("source_file", regexp_extract(input_file_name(), r"([^/]+$)", 1))
             .withColumn("imported_date", current_timestamp())
    )

    query = (
        df_stream.writeStream
                 .format("delta")
                 .outputMode("append")
                 .option("checkpointLocation", checkpoint_path)
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)   # process backlog, then stop
                 .start(table_location)
    )
    query.awaitTermination()
    spark.catalog.refreshTable(f"retail_raw.{name}")
    print(f"=== {name} ingestion completed ===")


if __name__ == "__main__":
    cfg = load_config()
    spark = build_spark(cfg)

    BASE = "file:/Users/Binaya/Documents/spark1"
    log("Starting pipeline...")


    # ---------------- Customers ----------------
    customer_schema = StructType([
        StructField("customer_id",   StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("gender",        StringType(), True),
        StructField("age",           IntegerType(), True),
        StructField("email",         StringType(), True),
        StructField("phone_number",  StringType(), True),
        StructField("city",          StringType(), True),
        StructField("loyalty_tier",  StringType(), True),
    ])
    ingest_entity(spark, "customer",
                  f"{BASE}/data/customers",
                  f"{BASE}/data/retail_raw/customer",
                  f"{BASE}/data/checkpoints/customer_bronze",
                  customer_schema)

    # ---------------- Employees ----------------
    employee_schema = StructType([
        StructField("employee_id",    StringType(), True),
        StructField("employee_code",  StringType(), True),
        StructField("store_id",       StringType(), True),
        StructField("store_name",     StringType(), True),
        StructField("effective_from", DateType(),   True),
        StructField("effective_to",   DateType(),   True),
        StructField("is_current",     BooleanType(), True),
        StructField("is_primary",     BooleanType(), True),
    ])
    ingest_entity(spark, "employee",
                  f"{BASE}/data/employees",
                  f"{BASE}/data/retail_raw/employee",
                  f"{BASE}/data/checkpoints/employee_bronze",
                  employee_schema)

    # ---------------- Products ----------------
    product_schema = StructType([
        StructField("product_id",    StringType(), True),
        StructField("product_name",  StringType(), True),
        StructField("supplier_id",   StringType(), True),
        StructField("supplier_name", StringType(), True),
        StructField("return_policy", StringType(), True),
        StructField("warranty",      StringType(), True),
    ])
    ingest_entity(spark, "product",
                  f"{BASE}/data/products",
                  f"{BASE}/data/retail_raw/product",
                  f"{BASE}/data/checkpoints/product_bronze",
                  product_schema)

    # ---------------- Stores ----------------
    store_schema = StructType([
        StructField("store_id",   StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("city",       StringType(), True),
    ])
    ingest_entity(spark, "store",
                  f"{BASE}/data/stores",
                  f"{BASE}/data/retail_raw/store",
                  f"{BASE}/data/checkpoints/store_bronze",
                  store_schema)

    # ---------------- Suppliers ----------------
    supplier_schema = StructType([
        StructField("supplier_id",   StringType(), True),
        StructField("supplier_name", StringType(), True),
    ])
    ingest_entity(spark, "supplier",
                  f"{BASE}/data/suppliers",
                  f"{BASE}/data/retail_raw/supplier",
                  f"{BASE}/data/checkpoints/supplier_bronze",
                  supplier_schema)

    # ---------------- Sales ----------------
    sale_schema = StructType([
        StructField("sale_id",          StringType(), True),
        StructField("sale_ts",          TimestampType(), True),
        StructField("customer_id",      StringType(), True),
        StructField("product_id",       StringType(), True),
        StructField("store_id",         StringType(), True),
        StructField("employee_id",      StringType(), True),
        StructField("quantity",         IntegerType(), True),
        StructField("unit_price",       DoubleType(), True),
        StructField("coupon_code",      StringType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("mode",             StringType(), True),
    ])
    ingest_entity(spark, "sales_raw",
                  f"{BASE}/data/sales",
                  f"{BASE}/data/retail_raw/sales_raw",
                  f"{BASE}/data/checkpoints/sale_bronze",
                  sale_schema)
    log("Pipeline finished.")

##------------------------------------------------------------------------------------------------------------------------------##
#python scripts/run_bronze.py
# PYTHONPATH=./src python scripts/run_bronze.py
# PYTHONPATH=$(pwd) python scripts/bronze_runner.py
