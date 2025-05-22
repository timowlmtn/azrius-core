#!/usr/bin/env python3
import json
import sys

import boto3
from botocore.exceptions import ClientError

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
BUCKET = "azri.us"
PREFIX = "agent/"  # S3 prefix under the bucket

# list of dates you want to create dummy JSON for
DATES = [
    "2025-07-07",
    "2025-05-19",
    "2025-06-23",
    "2025-06-02",
    # … add as many as you like
]

# optional: if you want to include a type (e.g. min/max) per date,
# you could switch DATES to a dict, e.g.
# DATES = {
#     "2025-07-07": "min",
#     "2025-08-15": "max",
#     ...
# }
DEFAULT_TYPE = "min"

s3 = boto3.client("s3")


def upload_dummy_for_date(date_str: str, typ: str = DEFAULT_TYPE):
    """Build a dummy payload for a given date and upload it to S3."""
    key = f"{PREFIX}{date_str}_{typ}.json"
    payload = {
        "date": date_str,
        "type": typ,
        "definition": f"Placeholder agent definition for {date_str} ({typ})",
    }
    body = json.dumps(payload, indent=2)
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
            ACL="public-read",
        )
        print(f"Uploaded → s3://{BUCKET}/{key}")
    except ClientError as e:
        print(f"ERROR uploading {key}: {e}", file=sys.stderr)


def main():
    for date_str in DATES:
        upload_dummy_for_date(date_str)


if __name__ == "__main__":
    main()
