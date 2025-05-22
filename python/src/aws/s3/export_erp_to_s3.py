#!/usr/bin/env python3
import os
import sys
import json
import logging
import argparse
from urllib.parse import urlparse

import boto3
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_s3_url(s3_url):
    """
    Given s3://bucket/prefix, returns (bucket, prefix)
    """
    parsed = urlparse(s3_url)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URL: {s3_url}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def get_tables(api_base, schema):
    resp = requests.get(f"{api_base}/api/erp/{schema}/tables")
    resp.raise_for_status()
    tables = resp.json()
    logger.info(f"Found {len(tables)} tables in schema '{schema}'")
    return tables


def get_rows(api_base, schema, table):
    resp = requests.get(f"{api_base}/api/erp/{schema}/tables/{table}/rows")
    resp.raise_for_status()
    rows = resp.json()
    logger.info(f"  → Retrieved {len(rows)} rows from table '{table}'")
    return rows


def upload_json_to_s3(s3_client, bucket, key, data):
    """
    Uploads a Python object as JSON to S3 under bucket/key.
    """
    body = json.dumps(data, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
    logger.info(f"  → Uploaded to s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Export ERP tables to S3 as JSON")
    parser.add_argument(
        "--schema", required=True, help="Name of the ERP schema to export"
    )
    args = parser.parse_args()

    api_base = os.environ.get("AZRIUS_API_URL")
    s3_data = os.environ.get("AZRIUS_S3_DATA")
    if not api_base or not s3_data:
        logger.error(
            "Environment variables AZRIUS_API_URL and AZRIUS_S3_DATA must be set"
        )
        sys.exit(1)

    bucket, prefix = parse_s3_url(s3_data)
    s3 = boto3.client("s3")

    tables = get_tables(api_base, args.schema)
    for table in tables:
        rows = get_rows(api_base, args.schema, table)
        key = f"{prefix.rstrip('/')}/{args.schema}/{table}.json"
        upload_json_to_s3(s3, bucket, key, rows)


if __name__ == "__main__":
    main()
