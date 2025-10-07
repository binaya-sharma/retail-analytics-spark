# Apache Spark Retail Analytics

A retail analytics pipeline built using **Apache Spark** and **Delta Lake**, structured around the **Medallion Architecture (Bronze → Silver → Gold)**.
It processes retail data across multiple dimensions: customers, employees, products, stores, suppliers, and sales to build an incremental, auditable data model for analytics and reporting.
The pipeline integrates with **Airbyte** for data ingestion, **AWS S3** for storage, and **Apache Airflow**(optional) for orchestration, and is designed to scale to a production-grade environment.

## Architecture

### Layers Overview
- **Bronze:** Raw ingestion layer containing unprocessed data from various retail sources (CSV/JSON).
- **Silver:** Transformed and standardized layer with deduplicated and cleaned data for consistency.
- **Gold:**
  - **Dimensions:**
    - `dim_customer` → SCD2
    - `dim_employee` → SCD2
    - `dim_product` → SCD1
    - `dim_store` → SCD1
    - `dim_supplier` → SCD1
    - `dim_date` → Calendar dimension
  - **Fact:**
    - `fact_sales` → Consolidates sales metrics by joining dimension tables and calculating gross, net, and discount amounts.

## Technology Stack
- **Apache Spark 4.0.0**
- **Delta Lake** (ACID compliance, schema enforcement, time travel)
- **Python (PySpark)**
- **SQL**
- **Derby Hive Metastore** (local metadata persistence)

## Data Flow

1. **Raw Data Landing:** Raw source files (CSV/JSON) are ingested into **AWS S3 (Bronze Layer)**.
2. **ETL with Spark + Delta Lake:** Data is cleaned, deduplicated, validated, and modeled into **Silver (curated)** and **Gold (analytics)** layers.
3. **Incremental Processing:** Implemented through **SCD1/SCD2** patterns and **audit columns** to ensure traceability.
4. **Data Storage:** Processed data is stored in **Delta format**, enabling ACID transactions and time-travel capabilities.
5. **Data Validation:** Schema enforcement and data quality checks are embedded in the transformation process.
6. **Future Integration:**
   - Delta tables can be integrated with **Snowflake** using the **Spark–Snowflake Connector**.
   - Incremental data loads are synchronized via **MERGE operations** for downstream analytics.
7. **Orchestration (Optional):** Can be automated using **Airflow** for scheduled execution.

## Future Enhancements

- **Data Orchestration:** Integration with Airflow / Databricks Workflows for production deployment.
- **Data Lakehouse Integration:** Full integration with S3 and Airbyte for real-time and batch ingestion.
- **Archival Strategy:** Automate data lifecycle management by archiving older datasets to cold storage.
- **Semantic Layer:** Build curated views for BI tools such as Power BI or Tableau.
- **Monitoring & Governance:** Introduce automated data quality checks, schema drift handling, and logging.

## Notes

- The current setup runs locally for simplicity and reproducibility.
- The pipeline can be extended to push incremental updates to **Snowflake** using the **Spark–Snowflake Connector**:
  - Delta Lake manages incremental logic (MERGE, CDC, audit tracking).
  - Spark writes only changed records to a Snowflake staging table.
  - A **MERGE SQL** operation inside Snowflake updates the final analytics warehouse.
  - This pattern supports **idempotent**, **incremental**, and **automated** synchronization between the lake and warehouse.
