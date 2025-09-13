from datetime import timedelta, datetime, timezone
import io
import logging
import boto3
import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# ----------------------
# Config
# ----------------------
RAW_BUCKET   = "superstore-etl-project"
RAW_PREFIX   = "superstore"   # s3://<bucket>/superstore/dt=YYYY-MM-DD/*.csv
MASTER_CSV   = "/opt/airflow/data/superstore.csv"
SPARK_SCRIPT = "/opt/airflow/dags/transform_superstore.py"
AWS_REGION   = "us-east-1"

log = logging.getLogger(__name__)

# ----------------------
# Helper: slice master CSV by Airflow date and push to S3
# ----------------------
def make_daily_and_upload(ds: str, **_kwargs) -> str:
    ymd = ds.replace("-", "")
    log.info("Reading master CSV from %s", MASTER_CSV)
    df = pd.read_csv(MASTER_CSV)

    # Normalize date columns
    df["Order Date"] = pd.to_datetime(df["Order Date"], format="%m/%d/%Y", errors="coerce")
    if "Ship Date" in df.columns:
        df["Ship Date"] = pd.to_datetime(df["Ship Date"], format="%m/%d/%Y", errors="coerce")

    # Slice partition
    ds_date = pd.to_datetime(ds).date()
    day = df.loc[df["Order Date"].dt.date == ds_date].copy()
    if day.empty:
        log.warning("No rows found for %s; skipping this run.", ds)
        raise AirflowSkipException(f"No data for {ds}")

    # Upload to S3
    key = f"{RAW_PREFIX}/dt={ds}/superstore_orders_{ymd}.csv"
    buf = io.StringIO()
    day.to_csv(buf, index=False)
    boto3.client("s3", region_name=AWS_REGION).put_object(
        Bucket=RAW_BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv"
    )
    return f"s3://{RAW_BUCKET}/{key}"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------
# DAG definition
# ----------------------
with DAG(
    dag_id="superstore_ingest_elt_snowflake",
    description="Superstore ELT: ingest → validate → transform → Snowflake → DQ",
    start_date=datetime(2015, 1, 2, tzinfo=timezone.utc),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["superstore", "elt", "snowflake"],
    is_paused_upon_creation=False,
) as dag:

    # Step 1: Upload partitioned raw data to S3
    ingest_upload = PythonOperator(
        task_id="ingest_upload_raw_csv",
        python_callable=make_daily_and_upload,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Step 2: Wait for Lambda validator to confirm _ok.json
    wait_for_ok = S3KeySensor(
        task_id="wait_for_ok",
        aws_conn_id="aws_default",
        bucket_name=RAW_BUCKET,
        bucket_key=RAW_PREFIX + "/dt={{ ds }}/_ok.json",
        poke_interval=30,
        timeout=30 * 60,
        mode="reschedule",
    )

    # Step 3: Transform raw CSV → curated Parquet with Spark
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && "
            "export PATH=$JAVA_HOME/bin:$PATH && "
            f"export AWS_DEFAULT_REGION={AWS_REGION} && "
            "export RUN_DS='{{ ds }}' && "
            f"python {SPARK_SCRIPT}"
        ),
    )

    # Step 4: Load curated data into Snowflake
    snowflake_copy = SnowflakeOperator(
        task_id="snowflake_copy",
        snowflake_conn_id="snowflake_default",
        sql=[
            "USE ROLE SYSADMIN;",
            "ALTER WAREHOUSE COMPUTE_WH RESUME IF SUSPENDED;",
            "USE WAREHOUSE COMPUTE_WH;",
            "USE DATABASE SUPERSTORE;",
            "USE SCHEMA ANALYTICS;",
            "CALL LOAD_SUPERSTORE_PARTITION('{{ ds }}');",
        ],
    )

    # Step 5: Data quality check – ensure rows landed
    dq_check = SnowflakeOperator(
        task_id="dq_check",
        snowflake_conn_id="snowflake_default",
        sql=[
            "USE ROLE SYSADMIN;",
            "USE WAREHOUSE COMPUTE_WH;",
            "USE DATABASE SUPERSTORE;",
            "USE SCHEMA ANALYTICS;",
            (
                "SELECT CASE WHEN COUNT(*)>0 THEN 1 ELSE 0 END AS ok "
                "FROM FACT_ORDER_LINE WHERE dt='{{ ds }}';"
            ),
        ],
    )

    ingest_upload >> wait_for_ok >> spark_transform >> snowflake_copy >> dq_check
