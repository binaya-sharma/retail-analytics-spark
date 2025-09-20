# Apache Spark Retail Analytics  

A retail analytics pipeline built on **Apache Spark** and **Delta Lake**, following the **Medallion Architecture (Bronze → Silver → Gold)**.  

A retail analytics pipeline leveraging Apache Spark and Delta Lake, structured around the Medallion Architecture (Bronze → Silver → Gold).
It ingests raw retail data, applies cleaning and deduplication processes, and produces dimension tables (SCD1 & SCD2) along with fact tables that are analytics-ready.
The pipeline is designed to seamlessly integrate with Airbyte for ingestion, AWS S3 for storage, and Apache Airflow for orchestration.

---

## Architecture

**Layers:**  
- **Bronze:** Raw ingestion of customers, employees, products, stores, suppliers, sales.  
- **Silver:** Cleaned, deduplicated, standardized staging tables.  
- **Gold:**  
  - **Dimensions**:  
    - `dim_customer` → SCD2  
    - `dim_employee` → SCD2
    - `dim_product` → SCD1  
    - `dim_store` → SCD1  
    - `dim_supplier` → SCD1  
    - `dim_date` → Calendar dimension  
  - **Fact**:  
    - `fact_sales` → simple version working (joins dimensions, calculates gross/net/discounts).  

---

## Tech Stack  
- **Apache Spark 4.0.0**  
- **Delta Lake**  
- **Python (PySpark)**  
- **SQL**  
- **Derby Hive Metastore**

## Future Scope & Industry Alignment  
- **Data Orchestration**: Airflow / Databricks Workflows  
- **Data Lake Integration**: S3 with Airbyte ingestion  
- **Archival**: Move processed data to cold storage  
- **Semantic Layer**: Expose views for BI tools (Power BI, Tableau, dbt Semantic Layer)  
- **Dashboarding**: Build analytics dashboards  

## Project Flow
![Project Flow](docs/image%20copy.png)

## Notes
- The project is currently running in local mode for simplicity, but it can be seamlessly integrated with Airbyte, AWS S3, and Airflow for production-ready orchestration and data management.


