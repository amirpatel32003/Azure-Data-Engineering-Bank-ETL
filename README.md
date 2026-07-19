# Azure Data Engineering Banking ETL Pipeline

End-to-end cloud ETL pipeline simulating TD Bank's nightly data ingestion using **Medallion Architecture** (Bronze → Silver → Gold).

## Business Case
TD Bank receives messy customer, loan, and transaction data from multiple systems. This pipeline ingests, cleans, validates, transforms, and curates the data for analytics and fraud models.

## Architecture
- **Landing Zone**: Raw CSVs in ADLS Gen2
- **Azure Data Factory**: Orchestration (planned)
- **Azure Databricks (PySpark)**: Processing & Transformation
- **Delta Lake**: Bronze / Silver / Gold layers

## Technologies
- Azure Data Lake Storage Gen2
- Azure Databricks + PySpark
- Delta Lake
- Medallion Architecture


## Data Layers
- **Landing**: Raw messy data
- **Silver**: Cleaned, validated, deduplicated
- **Gold**: Aggregated customer analytics table with Risk Level

## Key Features Implemented
- Data quality rules (null handling, deduplication, validation)
- Type casting and cleaning
- Joins and aggregations for analytics
- Delta Lake for ACID compliance

## How to Run
1. Upload CSVs to ADLS Gen2 `landing/` folder
2. Run `banking_etl_pipeline.py` in Databricks (cluster with ADLS access)
3. Check Delta tables in `silver/` and `gold/`

## Interview Talking Points
- Bronze/Silver/Gold Medallion Architecture for enterprise data pipelines
- Data quality enforcement in ETL
- Scalable PySpark transformations on Databricks
- Delta Lake for reliability and time travel

**Project demonstrates real enterprise data engineering practices used at banks**
