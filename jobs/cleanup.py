from app import load_config, build_spark
from utils.logger import get_logger

logger = get_logger('bronze_layer_log')

def log(message):
    if logger:
        logger.info(message)
    else:
        print(message)

if __name__ == "__main__":
    cfg = load_config()
    spark = build_spark(cfg)

    log("Starting pipeline cleanup...")

    tables = [
        #silver_layer
        "retail_silver.customer_clean",
        "retail_silver.employee_clean_history",
        "retail_silver.product_clean",
        "retail_silver.sales_clean",
        "retail_silver.store_clean",
        "retail_silver.supplier_clean",
        
        #gold_layer
        "retail_gold.dim_customer",
        "retail_gold.dim_employee",
        "retail_gold.dim_product",
        "retail_gold.dim_store",
        "retail_gold.dim_supplier",
        "retail_gold.dim_date",
        "retail_gold.fact_sales",

    ]

    for t in tables:
        try:
            spark.sql(f"DELETE FROM {t}")
            log(f"Cleared data from {t}")
        except Exception as e:
            log(f"Could not clear {t}: {e}")

    log("All cleanup complete.")