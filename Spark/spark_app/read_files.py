from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/home/jovyan/work/output/orders_parquet")
df.show()
