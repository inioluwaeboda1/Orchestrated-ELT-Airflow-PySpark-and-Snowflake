# transform_superstore.py
# --------------------------------------------------------------
# PySpark job for Superstore ELT pipeline
# - Reads daily raw CSV partition from S3 (dt=YYYY-MM-DD)
# - Normalizes dates and derives surrogate keys
# - Casts numeric/string fields consistently
# - Outputs curated Parquet partition aligned with Snowflake schema
# --------------------------------------------------------------

import os
from pyspark.sql import SparkSession, functions as F

# Config (injected from Airflow as env vars)
RAW_BUCKET     = os.environ.get("RAW_BUCKET", "superstore-etl-project")
RAW_PREFIX     = os.environ.get("RAW_PREFIX", "superstore")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")
RUN_DS         = os.environ["RUN_DS"]  # Airflow logical date: YYYY-MM-DD

# Initialize Spark (force UTC for reproducible timestamps)
spark = (
    SparkSession.builder.appName("superstore-transform")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Input and output S3 paths
raw_path = f"s3a://{RAW_BUCKET}/{RAW_PREFIX}/dt={RUN_DS}/"
out_path = f"s3a://{RAW_BUCKET}/{CURATED_PREFIX}/fact_order_line/dt={RUN_DS}/"

# Ingest raw CSV (schema inferred from header row)
df = spark.read.option("header", True).csv(raw_path)

# Transformations
# - Parse order/ship dates
# - Create surrogate date keys (yyyyMMdd)
# - Cast sales and postal_code
# - Add partition column (dt)
df = (
    df.withColumn("order_date", F.to_date(F.col("Order Date"), "M/d/y"))
      .withColumn("ship_date",  F.to_date(F.col("Ship Date"), "M/d/y"))
      .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
      .withColumn("ship_date_key",  F.date_format("ship_date",  "yyyyMMdd").cast("int"))
      .withColumn("sales", F.col("Sales").cast("double"))
      .withColumn("postal_code", F.col("Postal Code").cast("string"))
      .withColumn("dt", F.to_date(F.lit(RUN_DS)))
)

# Align with Snowflake FACT_ORDER_LINE schema
out = df.select(
    F.col("Order ID").alias("order_id"),
    F.col("order_date_key"),
    F.col("ship_date_key"),
    F.col("Customer ID").alias("customer_id"),
    F.col("Product ID").alias("product_id"),
    F.col("Ship Mode").alias("ship_mode"),
    F.col("City").alias("city"),
    F.col("State").alias("state"),
    F.col("postal_code"),
    F.col("sales"),
    F.col("dt"),
)

# Write curated partition as Parquet
# - coalesce(1): single file per day (simplifies downstream load)
# - overwrite: idempotent for retries/backfills
out.coalesce(1).write.mode("overwrite").parquet(out_path)

spark.stop()
