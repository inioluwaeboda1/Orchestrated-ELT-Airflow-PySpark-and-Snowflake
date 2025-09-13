Superstore ELT Pipeline
=======================

**This project delivers a modern end-to-end ELT pipeline that ingests daily Superstore data into AWS S3, validates it with Lambda, transforms it into curated Parquet files using PySpark, and loads it into Snowflake for analytics. It highlights workflow orchestration with Airflow, scalable cloud transformations, and SQL-based business insights.**

🚀 Architecture & Flow
----------------------

1.  **Ingestion** – A master Superstore CSV is split daily by Airflow and uploaded to S3 (dt=YYYY-MM-DD/).
    
2.  **Validation** – An AWS Lambda function validates schema + row counts and writes an \_ok.json marker.
    
3.  **Transformation** – PySpark normalizes and converts raw CSVs into partitioned Parquet files.
    
4.  **Load** – Airflow triggers a Snowflake stored procedure to copy curated partitions into FACT\_ORDER\_LINE.
    
5.  **Analytics** – SQL queries in Snowflake provide sales trends, top customers, and regional performance insights.
    

🛠️ Tech Stack
--------------

*   **Orchestration**: Apache Airflow (Dockerized)
    
*   **Storage**: AWS S3 (raw + curated layers)
    
*   **Validation**: AWS Lambda (Python)
    
*   **Processing**: PySpark (partitioned Parquet)
    
*   **Warehouse**: Snowflake (external stage + SQL procedures)
    

📂 Project Structure
--------------------

```text
.
├── airflow/                 # DAGs, plugins, data, requirements
├── lambda_validator.py      # AWS Lambda validation function
├── transform_superstore.py  # PySpark transformation job
├── snowflake_analysis.sql   # DDL + analytics queries
├── docker-compose.yml       # Local Airflow setup
└── README.md

```

🔍 Key Highlights
-----------------

*   End-to-end ELT simulation with **production-style orchestration**.
    
*   **Data quality guardrails** with Lambda and Airflow sensors.
    
*   **Idempotent, partition-based loads** for repeatable runs.
    
*   Showcases both **data engineering practices** (pipelines, orchestration, staging) and **analytics skills** (Snowflake SQL).
    

📊 Example Insights
-------------------

*   **Daily Sales Trends** across all dates
    
*   **Top 10 Customers** by revenue
    
*   **Top 10 States** driving performance
    
*   **Year-over-Year Monthly Sales Growth**
