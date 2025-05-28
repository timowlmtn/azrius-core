from pyspark.sql import SparkSession
import os

AWS_ACCESS_KEY = os.getenv("AWS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise EnvironmentError("Missing AWS credentials")

spark = (
    SparkSession.builder.appName("IcebergS3Test")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://azri.us-spark/data")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

df = spark.createDataFrame([{"id": 1, "label": "apple"}, {"id": 2, "label": "banana"}])

df.writeTo("iceberg.default.test_table").using("iceberg").createOrReplace()

df_read = spark.read.table("iceberg.default.test_table")
df_read.show()

spark.stop()
