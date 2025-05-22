#!/usr/bin/env python

import os
import argparse
import json
import ast

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from snowflake.connector.pandas_tools import write_pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

import dotenv
from core.db import connect

spark = (
    SparkSession.builder.appName("csv_to_postgres")
    .config("spark.jars", os.getenv("POSTGRES_SPARK_JARS", None))
    .getOrCreate()
)


def create_schema_if_not_exists(cur, db_type, schema):
    if db_type == "postgres":
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    else:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(f"USE SCHEMA {schema};")


def create_table_if_not_exists(cur, db_type, schema, table, columns):
    if db_type == "postgres":
        cols_ddl = ",\n    ".join(f"{col} text" for col in columns)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id bigserial PRIMARY KEY,
            {cols_ddl},
            created_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
            created_by text DEFAULT CURRENT_USER,
            updated_by text DEFAULT CURRENT_USER
        );
        """
    else:
        cols_ddl = ",\n    ".join(f"{col} TEXT" for col in columns)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id BIGINT AUTOINCREMENT,
            {cols_ddl},
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            created_by VARCHAR DEFAULT CURRENT_USER(),
            updated_by VARCHAR DEFAULT CURRENT_USER()
        );
        """
    cur.execute(ddl)


def load_csv_pandas(conn, db_type, csv_path, schema, table, truncate, create):
    df = pd.read_csv(csv_path)
    cur = conn.cursor()

    # 1. Ensure schema exists
    create_schema_if_not_exists(cur, db_type, schema)
    print(f"✔ Schema {schema} ensured.")

    # 2. Create table if needed
    if create:
        create_table_if_not_exists(cur, db_type, schema, table, df.columns)
        print(f"✔ Created {schema}.{table}")

    # 3. Truncate if requested
    if truncate:
        if db_type == "postgres":
            cur.execute(f"TRUNCATE {schema}.{table} RESTART IDENTITY CASCADE;")
        else:
            cur.execute(f"TRUNCATE TABLE {schema}.{table};")
        print(f"🗑 Truncated {schema}.{table}")

    # 4. Bulk load
    if db_type == "postgres":
        cols = df.columns.tolist()
        values = df.values.tolist()
        insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(cols)}) VALUES %s"
        execute_values(cur, insert_sql, values, page_size=1000)
        conn.commit()
        nrows = len(df)
    else:
        df.columns = df.columns.str.upper()
        success, _, nrows, _ = write_pandas(
            conn, df, table_name=table.upper(), schema=schema.upper()
        )
        if not success:
            raise RuntimeError(f"write_pandas failed for {schema}.{table}")

    cur.close()
    print(
        f"➕ Loaded {nrows} rows into {schema}.{table} from {os.path.basename(csv_path)}"
    )


def get_spark(app_name="csv_to_postgres"):
    return (
        SparkSession.builder.appName(app_name)
        # .config("spark.jars", "/path/to/postgresql.jar")
        .getOrCreate()
    )


def load_csv_spark(
    csv_path, schema, table, truncate, create, spark, date_format="yyyy-MM-dd"
):

    # Read CSV in parallel
    df_spark = (
        spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    )
    # explicitly parse the "date" column (adjust the format to your data)
    df_spark = df_spark.withColumn("date", to_date(col("date"), date_format))

    # Use psycopg2 for DDL and truncation
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "azrius"),
        user=os.getenv("POSTGRES_USER", "azrius_user"),
        password=os.getenv("POSTGRES_PASSWORD", "azrius_password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    cur = conn.cursor()
    create_schema_if_not_exists(cur, "postgres", schema)
    conn.commit()
    print(f"✔ Schema {schema} ensured.")

    if create:
        create_table_if_not_exists(cur, "postgres", schema, table, df_spark.columns)
        conn.commit()
        print(f"✔ Created {schema}.{table}")

    if truncate:
        cur.execute(f"TRUNCATE {schema}.{table} RESTART IDENTITY CASCADE;")
        conn.commit()
        print(f"🗑 Truncated {schema}.{table}")

    cur.close()
    conn.close()

    # Write via JDBC
    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}"
        f":{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'azrius')}"
    )
    (
        df_spark.repartition(10)
        .write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{schema}.{table}")
        .option("user", os.getenv("POSTGRES_USER", "azrius_user"))
        .option("password", os.getenv("POSTGRES_PASSWORD", "azrius_password"))
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 50000)
        .mode("append")
        .save()
    )
    print(f"➕ Spark loaded into {schema}.{table} from {os.path.basename(csv_path)}")


def main():
    parser = argparse.ArgumentParser(
        description="Load CSVs into Postgres or Snowflake via filename→table mapping."
    )
    parser.add_argument(
        "--db_type",
        choices=["postgres", "snowflake"],
        required=True,
        help="Target database type.",
    )
    parser.add_argument(
        "--use-spark",
        action="store_true",
        help="Use Spark (parallel) loader for Postgres instead of pandas.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--csv_file", type=str, help="Path to a single CSV to load.")
    group.add_argument("--load_dir", type=str, help="Directory of CSVs to load.")
    parser.add_argument(
        "--date_format", type=str, default="yyyy-MM-dd", help="Date format for Spark."
    )
    parser.add_argument("--schema", type=str, required=True, help="Target schema.")
    parser.add_argument(
        "--table_name",
        type=str,
        required=True,
        help="JSON or Python dict mapping filenames to table names.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="If set, truncate each table before loading.",
    )
    args = parser.parse_args()

    # Parse the mapping dictionary
    try:
        mapping = json.loads(args.table_name)
    except json.JSONDecodeError:
        mapping = ast.literal_eval(args.table_name)

    # Build list of (path, table) tasks
    tasks = []
    if args.csv_file:
        fname = os.path.basename(args.csv_file)
        tbl = mapping.get(fname) or mapping
        tasks.append((args.csv_file, tbl))
    else:
        for fname, tbl in mapping.items():
            path = os.path.join(args.load_dir, fname)
            if os.path.isfile(path):
                tasks.append((path, tbl))
            else:
                print(f"⚠️ Skipping missing file: {fname}")

    if not tasks:
        print("❌ No files to load. Check your --load_dir and mapping.")
        exit(1)

    # If using Spark, only valid for Postgres
    spark = None
    if args.use_spark:
        if args.db_type != "postgres":
            print("❌ --use-spark is only supported with --db_type=postgres.")
            exit(1)
        spark = get_spark()

    # Connect once for pandas / Snowflake path
    conn = None
    if not args.use_spark or args.db_type == "snowflake":
        conn = connect(args.db_type)

    seen = {}
    for path, tbl in tasks:
        do_create = tbl not in seen
        do_truncate = args.truncate and tbl not in seen

        if args.use_spark:
            load_csv_spark(
                path, args.schema, tbl, do_truncate, do_create, spark, args.date_format
            )
        else:
            load_csv_pandas(
                conn, args.db_type, path, args.schema, tbl, do_truncate, do_create
            )

        seen[tbl] = True

    if conn:
        conn.close()
    if spark:
        spark.stop()


if __name__ == "__main__":
    main()
