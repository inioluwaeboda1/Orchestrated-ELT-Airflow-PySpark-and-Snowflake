-- snowflake_analysis.sql
-- ------------------------------------------------------------
-- Superstore ELT: Snowflake setup, loader proc, and analytics
-- - Creates warehouse / DB / schema (idempotent)
-- - Defines FACT_ORDER_LINE, S3 external stage + file format
-- - Stored proc loads a single dt partition from S3 Parquet
-- - Useful analytics queries for quick insights
-- ------------------------------------------------------------

-- === Context & Infra ===
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60;

CREATE DATABASE IF NOT EXISTS SUPERSTORE;
CREATE SCHEMA   IF NOT EXISTS SUPERSTORE.ANALYTICS;

USE WAREHOUSE COMPUTE_WH;
USE DATABASE  SUPERSTORE;
USE SCHEMA    ANALYTICS;

-- === Core Fact Table (matches PySpark output) ===
CREATE OR REPLACE TABLE FACT_ORDER_LINE (
  order_id        STRING,
  order_date_key  INTEGER,
  ship_date_key   INTEGER,
  customer_id     STRING,
  product_id      STRING,
  ship_mode       STRING,
  city            STRING,
  state           STRING,
  postal_code     STRING,
  sales           DOUBLE,
  dt              DATE
);

-- === External File Format & Stage (S3 via integration) ===
CREATE OR REPLACE FILE FORMAT FF_PARQUET_SUPERSTORE TYPE = PARQUET;

CREATE OR REPLACE STAGE STG_SUPERSTORE_CURATED
  URL='s3://superstore-etl-project/curated/'
  STORAGE_INTEGRATION = s3_superstore_data
  FILE_FORMAT = FF_PARQUET_SUPERSTORE;

-- Optional sanity checks (uncomment to inspect stage files)
-- LIST @STG_SUPERSTORE_CURATED/fact_order_line/dt=2015-01-04/;
-- SELECT * FROM @STG_SUPERSTORE_CURATED/fact_order_line/dt=2015-01-04/
--   ( FILE_FORMAT => FF_PARQUET_SUPERSTORE, PATTERN => '.*\\.parquet' )
--   LIMIT 5;

-- === Loader Procedure: copy single dt partition ===
CREATE OR REPLACE PROCEDURE LOAD_SUPERSTORE_PARTITION(RUN_DS STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Ensure context & capacity
  snowflake.execute({sqlText: "USE ROLE SYSADMIN"});
  snowflake.execute({sqlText: "ALTER WAREHOUSE COMPUTE_WH RESUME IF SUSPENDED"});
  snowflake.execute({sqlText: "USE WAREHOUSE COMPUTE_WH"});
  snowflake.execute({sqlText: "USE DATABASE SUPERSTORE"});
  snowflake.execute({sqlText: "USE SCHEMA ANALYTICS"});

  const stagePath = `@STG_SUPERSTORE_CURATED/fact_order_line/dt=${RUN_DS}/`;

  // Copy Parquet -> FACT_ORDER_LINE (column-name match, skip _SUCCESS)
  const copySql = `
    COPY INTO ANALYTICS.FACT_ORDER_LINE
    FROM ${stagePath}
      ( FILE_FORMAT => FF_PARQUET_SUPERSTORE
      , PATTERN     => '.*\\\\.parquet' )
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'ABORT_STATEMENT'
    FORCE    = TRUE
  `;
  snowflake.execute({sqlText: copySql});
  return 'OK: loaded ' + RUN_DS;
$$;

-- Quick smoke tests (uncomment to run manually)
-- CALL LOAD_SUPERSTORE_PARTITION('2015-01-04');
-- SELECT COUNT(*) AS rows_loaded FROM FACT_ORDER_LINE WHERE dt='2015-01-04';

-- ============================================================
--                     ANALYTICS QUERIES
-- ============================================================

-- 1) Daily sales trend
SELECT dt, ROUND(SUM(sales), 2) AS total_sales
FROM FACT_ORDER_LINE
GROUP BY dt
ORDER BY dt;

-- 2) Top 10 states by sales
SELECT state, ROUND(SUM(sales), 2) AS total_sales
FROM FACT_ORDER_LINE
GROUP BY state
ORDER BY total_sales DESC
LIMIT 10;

-- 3) Top 10 customers by sales
SELECT customer_id, ROUND(SUM(sales), 2) AS total_sales
FROM FACT_ORDER_LINE
GROUP BY customer_id
ORDER BY total_sales DESC
LIMIT 10;

-- 4) Monthly sales (YYYY-MM), convenient for dashboards
WITH m AS (
  SELECT TO_CHAR(dt, 'YYYY-MM') AS ym, SUM(sales) AS sales
  FROM FACT_ORDER_LINE
  GROUP BY 1
)
SELECT ym, ROUND(sales, 2) AS total_sales
FROM m
ORDER BY ym;
