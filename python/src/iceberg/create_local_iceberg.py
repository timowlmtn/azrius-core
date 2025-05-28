import os
from pyspark.sql import SparkSession

# ─── Set up a local warehouse ──────────────────────────────────────────────
WAREHOUSE_PATH = os.path.abspath("./data/iceberg_warehouse")

# ─── Build Spark Session with Iceberg ──────────────────────────────────────
spark = (
    SparkSession.builder.appName("LocalIcebergTest")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", f"file://{WAREHOUSE_PATH}")
    .getOrCreate()
)

# ─── Create and Write a Table ──────────────────────────────────────────────
df = spark.createDataFrame(
    [
        {"id": 1, "label": "apple"},
        {"id": 2, "label": "banana"},
        {"id": 3, "label": "cherry"},
    ]
)

print(f"[INFO] Writing to local Iceberg table at: {WAREHOUSE_PATH}/default/fruit_table")
df.writeTo("iceberg.default.fruit_table").using("iceberg").createOrReplace()

# ─── Read and Show the Table ───────────────────────────────────────────────
print("[INFO] Reading back the Iceberg table:")
result = spark.read.table("iceberg.default.fruit_table")
result.show()

spark.stop()
