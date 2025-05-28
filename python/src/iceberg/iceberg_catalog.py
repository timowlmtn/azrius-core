from pyspark.python.pyspark.shell import spark

spark.conf.set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.my_catalog.type", "hadoop")
spark.conf.set("spark.sql.catalog.my_catalog.warehouse", "s3://my-bucket/data")
