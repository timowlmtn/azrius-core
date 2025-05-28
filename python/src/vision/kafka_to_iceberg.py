import os
import json
import time
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import Row

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
ICEBERG_HOME = os.getenv("ICEBERG_HOME", "s3://my-bucket/iceberg")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shelf_detections")
CATALOG_NAME = "iceberg"

BATCH_SIZE = 100  # Number of messages before writing
BATCH_INTERVAL = 10  # Seconds to wait between empty batches


def get_spark_session():
    return (
        SparkSession.builder.appName("KafkaToIcebergManualConsumer")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,"
            "org.apache.hadoop:hadoop-aws:3.3.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.901",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config(
            "spark.sql.catalog.iceberg.warehouse", "s3a://azri.us-data/iceberg"
        )  # <- FIXED
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def write_batch(spark, catalog_name, records):
    print(
        f"[INFO] Writing {len(records)} records to Iceberg catalog '{catalog_name}'..."
    )
    if not records:
        return

    by_table = {}
    for record in records:
        print(f"[RECORD] {record}")
        table = record.pop("table", "default")
        by_table.setdefault(table, []).append(record)

    print(f"[INFO] Grouped records by table: {list(by_table.keys())}")

    for table_name, rows in by_table.items():
        print(f"[INFO] Writing to table '{table_name}' with {len(rows)} rows...")
        df = spark.createDataFrame([Row(**r) for r in rows])
        print(
            f"[INFO] ----------------------- Spark DataFrame Schema -----------------------"
        )

        if df.isEmpty():
            print(f"[WARN] No data to write for table '{table_name}', skipping...")
            continue

        full_table = f"{catalog_name}.default.{table_name}"
        print(f"[INFO] Writing to Iceberg table: {full_table}")

        (
            df.writeTo(full_table)
            .using("iceberg")
            .option("merge-schema", "true")
            .append()
        )

        print(f"[SUCCESS] Written {len(rows)} records to {full_table}")


def main():
    spark = get_spark_session()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    buffer = []
    last_write = time.time()

    for message in consumer:
        buffer.append(message.value)

        print(f"[RECEIVED] {len(buffer)} messages in buffer...")
        if len(buffer) >= BATCH_SIZE or (time.time() - last_write) >= BATCH_INTERVAL:
            write_batch(spark, CATALOG_NAME, buffer)
            buffer.clear()
            last_write = time.time()


if __name__ == "__main__":
    main()
