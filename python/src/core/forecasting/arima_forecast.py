#!/usr/bin/env python

import os
import argparse
import pickle
import gzip
import psycopg2
from statsmodels.tsa.statespace.sarimax import SARIMAX

from pyspark.sql import SparkSession
import pandas as pd
import boto3
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def get_spark(app_name="arima_model_executor"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("POSTGRES_SPARK_JARS", None))
        .getOrCreate()
    )


def create_schema_if_not_exists(cur, schema):
    ddl = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(ddl)


def create_model_table_if_not_exists(cur, schema, table):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        start_year    INTEGER   PRIMARY KEY,
        end_year      INTEGER,
        model_blob    BYTEA,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cur.execute(ddl)


def load_weekly_series_db(
    spark,
    jdbc_url,
    pg_props,
    schema,
    sales_table,
    date_col="date",
    value_col="sale_dollars",
):
    """
    Loads weekly‐aggregated sales directly from Postgres via JDBC.
    - dbtable is a subquery that truncates to week and sums sales.
    - Returns a pandas Series indexed by week-start (Monday).
    """
    # build the subquery; note the outer alias is required
    weekly_sql = f"""
      (
        SELECT
          date_trunc('week', {date_col})::date    AS week_start,
          SUM({value_col})                        AS sales
        FROM {schema}.{sales_table}
        GROUP BY week_start
        ORDER BY week_start
      ) AS weekly_sales
    """
    # Print the SQL for debugging
    print(f"SQL: {weekly_sql}")

    # read just the weekly aggregates
    df_weekly = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", weekly_sql)
        .option("user", pg_props["user"])
        .option("password", pg_props["password"])
        .option("driver", pg_props["driver"])
        .load()
    )

    # convert to pandas and enforce a weekly freq index
    pdf = (
        df_weekly.toPandas()
        .set_index("week_start")
        .pipe(lambda df: df.set_index(pd.to_datetime(df.index)))
        .asfreq("W-MON")  # weeks starting on Monday
    )

    return pdf["sales"]


def fit_arima(ts, order, seasonal_order):
    """
    Fit a SARIMA model to the time series data.
    order: (p, d, q) for ARIMA
    seasonal_order: (P, D, Q, m) for seasonal component

    """
    ts_clean = pd.to_numeric(ts, errors="coerce").fillna(0).astype("float64")

    model = SARIMAX(
        ts_clean,
        order=tuple(order),
        seasonal_order=tuple(seasonal_order),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    fitted = model.fit(disp=False)
    return fitted


import os
import pickle
import gzip
import boto3


def persist_model(
    schema, model_table, start_year, end_year, model_blob, store_local=True
):
    """
    Serialize, compress, optionally store locally, and upload the ARIMA model to S3.

    Args:
        schema (str): Postgres/S3 schema or folder prefix.
        model_table (str): Model table name or folder.
        start_year (int): First year in training data.
        end_year (int): Last year in training data.
        model_blob: The fitted SARIMAXResults object.
        store_local (bool): If True, write compressed pickle locally before uploading.
    """
    # 1) Serialize & compress the SARIMAXResults object
    raw_blob = pickle.dumps(model_blob, protocol=4)
    compressed = gzip.compress(raw_blob)

    # 2) Optionally write locally
    if store_local:
        # default directory or override with env var
        local_dir = os.getenv("ARIMA_MODEL_LOCAL_DIR", ".")
        os.makedirs(local_dir, exist_ok=True)
        filename = f"data/{schema}/{model_table}_{start_year}_{end_year}_arima.pkl.gz"
        local_path = os.path.join(local_dir, filename)
        with open(local_path, "wb") as f:
            f.write(compressed)
        print(f"💾 Saved ARIMA model locally to {local_path}")
    else:
        # 3) Configure S3 target
        bucket = os.getenv("ARIMA_MODEL_BUCKET")
        key = f"{schema}/{model_table}/{start_year}_{end_year}_arima.pkl.gz"
        s3 = boto3.client("s3")

        # 4) Upload the compressed pickle
        s3.put_object(Bucket=bucket, Key=key, Body=compressed)
        print(f"➕ Uploaded ARIMA model to s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(
        description="Fit and persist a seasonal ARIMA model via Spark + Statsmodels"
    )
    parser.add_argument("--pg-host", default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("POSTGRES_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("POSTGRES_DB", "azrius"))
    parser.add_argument("--pg-user", default=os.getenv("POSTGRES_USER", "azrius_user"))
    parser.add_argument(
        "--pg-pass", default=os.getenv("POSTGRES_PASSWORD", "azrius_password")
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Postgres schema for both sales and model tables",
    )
    parser.add_argument("--sales-table", required=True, help="Monthly sales table name")
    parser.add_argument(
        "--model-table", required=True, help="ARIMA model persistence table name"
    )
    parser.add_argument(
        "--order", default="1,1,1", help="ARIMA order p,d,q (comma-separated)"
    )
    parser.add_argument(
        "--seasonal-order",
        default="1,1,1,12",
        help="Seasonal order P,D,Q,m (comma-separated)",
    )
    parser.add_argument(
        "--start-year", type=int, default=None, help="Earliest year to include"
    )
    parser.add_argument(
        "--end-year", type=int, default=None, help="Latest year to include"
    )
    args = parser.parse_args()

    # Build JDBC URL and properties
    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    pg_props = {
        "user": args.pg_user,
        "password": args.pg_pass,
        "driver": "org.postgresql.Driver",
    }

    # 1) Start Spark
    spark = get_spark()

    # 2) Load the time series
    ts = load_weekly_series_db(spark, jdbc_url, pg_props, args.schema, args.sales_table)
    if args.start_year:
        ts = ts[ts.index.year >= args.start_year]
    if args.end_year:
        ts = ts[ts.index.year <= args.end_year]

    start_year, end_year = ts.index.min().year, ts.index.max().year
    print(f"→ Fitting ARIMA on {start_year}–{end_year}")

    print(f"→ Time series loaded: {len(ts)} months")
    print(ts)

    # 3) Fit the model
    order = list(map(int, args.order.split(",")))
    seasonal_order = list(map(int, args.seasonal_order.split(",")))
    fitted = fit_arima(ts, order, seasonal_order)
    print(f"→ ARIMA fitted: order={fitted.model_orders}")

    # 4) Serialize
    model_blob = pickle.dumps(fitted)

    # 5) Persist to Postgres
    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_pass,
    )
    cur = conn.cursor()
    create_schema_if_not_exists(cur, args.schema)
    conn.commit()
    print(f"✔ Schema {args.schema} exists")

    create_model_table_if_not_exists(cur, args.schema, args.model_table)
    conn.commit()
    print(f"✔ Model table {args.schema}.{args.model_table} exists")

    persist_model(args.schema, args.model_table, start_year, end_year, model_blob)
    conn.commit()
    print(
        f"➕ Persisted ARIMA model for {start_year}–{end_year} into {args.schema}.{args.model_table}"
    )

    # cleanup
    cur.close()
    conn.close()
    spark.stop()


if __name__ == "__main__":
    main()
