from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("OlistOrdersStreaming") \
    .getOrCreate()

# Define schema matching your order JSON
orders_schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", StringType()) \
    .add("order_approved_at", StringType()) \
    .add("order_delivered_carrier_date", StringType()) \
    .add("order_delivered_customer_date", StringType()) \
    .add("order_estimated_delivery_date", StringType())

# Read stream from Kafka
orders_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "orders_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract the JSON string from the Kafka 'value' field and parse it
orders_parsed = orders_raw.select(
    from_json(col("value").cast("string"), orders_schema).alias("order")
).select("order.*")

# Write streaming data to parquet files locally (for testing)
query = orders_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/home/jovyan/work/output/orders_parquet") \
    .option("checkpointLocation", "/home/jovyan/work/output/checkpoints/orders") \
    .start()

query.awaitTermination()
