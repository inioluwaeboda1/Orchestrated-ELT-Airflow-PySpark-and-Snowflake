# lambda_validator.py
# -------------------------------------------------------------------
# AWS Lambda function that validates daily Superstore CSV drops in S3.
# - Triggered automatically by S3 PUT events (new object creation).
# - Checks filename pattern, CSV structure, required columns, and data rows.
# - Writes either _ok.json or _fail.json marker into the same partition
#   so that Airflow can safely detect readiness via S3KeySensor.
# -------------------------------------------------------------------

import json
import os
import re
import csv
from io import StringIO
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

# -------------------------------------------------------------------
# Config (injected via Lambda env vars; defaults provided for local test)
# -------------------------------------------------------------------
RAW_BUCKET = os.environ.get("RAW_BUCKET", "superstore-etl-project")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "superstore")

# Enforce strict naming convention: superstore/dt=YYYY-MM-DD/superstore_orders_YYYYMMDD.csv
CSV_KEY_RE = re.compile(
    rf"^{re.escape(RAW_PREFIX)}/dt=\d{{4}}-\d{{2}}-\d{{2}}/superstore_orders_\d{{8}}\.csv$"
)

s3 = boto3.client("s3")


# -------------------------------------------------------------------
# Utility: write a JSON validation marker (_ok.json or _fail.json)
# This allows downstream systems (Airflow) to know partition status.
# -------------------------------------------------------------------
def _put_marker(prefix: str, ok: bool, message: str) -> None:
    key = f"{prefix}/{'_ok' if ok else '_fail'}.json"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        ContentType="application/json",
        Body=json.dumps({"ok": ok, "message": message}, indent=2).encode("utf-8"),
    )


# -------------------------------------------------------------------
# Utility: fetch CSV object from S3 and parse into rows.
# - Decodes as UTF-8 (with BOM handling).
# - Returns a list of rows (lists of strings).
# -------------------------------------------------------------------
def _read_csv_body(bucket: str, key: str) -> list[list[str]]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8-sig", errors="strict")
    return list(csv.reader(StringIO(body)))


# -------------------------------------------------------------------
# Main Lambda entrypoint
# -------------------------------------------------------------------
def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    Processes S3 event notifications and validates uploaded daily CSVs.

    Validation logic:
    - Ignore irrelevant buckets/keys.
    - Ensure file is non-empty and has at least one data row.
    - Verify required columns exist.
    - Write _ok.json or _fail.json marker alongside the file.
    """
    records = event.get("Records", [])
    for rec in records:
        # Defensive parsing of S3 event payload
        try:
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
        except KeyError:
            continue  # skip malformed event

        # Only validate our specific bucket + prefix pattern
        if bucket != RAW_BUCKET or not CSV_KEY_RE.match(key):
            continue

        # Partition folder (e.g. superstore/dt=2015-01-04)
        partition_prefix = key.rsplit("/", 1)[0]

        try:
            rows = _read_csv_body(bucket, key)
        except ClientError as e:
            _put_marker(partition_prefix, False, f"S3 error: {e.response['Error']['Message']}")
            continue
        except UnicodeDecodeError:
            _put_marker(partition_prefix, False, "Decode error: expected UTF-8 CSV")
            continue

        # Validation: must contain header + at least 1 data row
        if len(rows) <= 1:
            _put_marker(partition_prefix, False, "No data rows found")
            continue

        # Check required columns are present
        header = [h.strip().lower() for h in rows[0]]
        required = {"order id", "order date", "customer id", "product id", "sales"}
        missing = sorted(required - set(header))
        if missing:
            _put_marker(partition_prefix, False, f"Missing columns: {missing}")
            continue

        # Success: write marker with row count
        _put_marker(partition_prefix, True, f"Validated {len(rows) - 1} rows")

    return {"statusCode": 200, "body": "ok"}
