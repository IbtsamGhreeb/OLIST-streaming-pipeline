# ******************************************************************
#               IMPORTING LIBERERIES
# ******************************************************************
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import from_json
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, TimestampType, DoubleType

# ******************************************************************
#                 SETTING VARIABLES
# ******************************************************************

ELASTIC_HOST = "172.17.240.1"
ELASTIC_PORT = 9200
ELASTIC_URL = f"http://{ELASTIC_HOST}:{ELASTIC_PORT}"
PATH = r"/mnt/d/project/olst/pandas transformation/Transformations"

# ******************************************************************
#                 STARTING SPARK SESSION
# ******************************************************************

spark_stream_session = SparkSession.builder.appName(
    'Spark_elastic').master('local')\
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("es.nodes", "172.17.240.1") \
    .config("es.port", "9200") \
    .config("es.nodes.wan.only", "true") \
    .getOrCreate()

streamed_df = spark_stream_session.readStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', '172.17.247.58:9092')\
    .option('subscribe', 'Store_Topic')\
    .option('startingOffsets', 'earliest')\
    .load()

streamed_df = streamed_df.select(
    func.col('key').cast('string'),
    func.col('value').cast('string')
)
schema = schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('order_purchase_timestamp', TimestampType(), True),
    StructField('order_approved_at', TimestampType(), True),
    StructField('order_delivered_carrier_date', TimestampType(), True),
    StructField('order_delivered_customer_date', TimestampType(), True),
    StructField('order_estimated_delivery_date', TimestampType(), True),

    StructField('order_item_id', DoubleType(), True),
    StructField('product_id', StringType(), True),
    StructField('seller_id', StringType(), True),
    StructField('seller_limit_date_to_ship_to_vendor', TimestampType(), True),

    StructField('price', DoubleType(), True),
    StructField('freight_value', DoubleType(), True),

    StructField('customer_unique_id', StringType(), True),
    StructField('customer_city', StringType(), True),
    StructField('customer_state', StringType(), True),

    StructField('payment_sequential', DoubleType(), True),
    StructField('payment_type', StringType(), True),
    StructField('payment_installments', DoubleType(), True),
    StructField('payment_value', DoubleType(), True),

    StructField('product_category_name', StringType(), True),
    StructField('seller_city', StringType(), True),
    StructField('seller_state', StringType(), True)
])

parased_df = streamed_df.select(from_json(func.col('value'), schema=schema).alias('data'))\
    .select('data.*')
# ******************************************************************
#                   SPARK TRANSFORMATIONS
# ******************************************************************

delivery_time_df = parased_df.withColumn('is_success',
                                         func.when(func.lower(func.col('order_status')).isin('delivered', 'shipped', 'invoiced'), 1).otherwise(0)).\
    na.fill('not_defined', 'payment_type')

payment_type_fix_df = delivery_time_df.withColumn('payment_values',
                                                  func.when(func.col('is_success') == 0, 0).
                                                  when(func.col('payment_value').isNull() &
                                                       (func.col('is_success') == 1), func.col(
                                                      'price') + func.col('freight_value')
                                                  ).otherwise(func.col('payment_value'))
                                                  ).drop(func.col('payment_value'))

# fix nulls in order_delivered_customer_date if is_success = 1 & date = null then date = carrierdate + 2 days
order_delivered_date_fix = payment_type_fix_df.withColumn('orders_delivered_customer_date', func.when((func.col('is_success') == 1) &
                                                                                                      (func.col('order_delivered_customer_date').isNull()) &
                                                                                                      (func.col(
                                                                                                          'order_delivered_carrier_date').isNull()),
                                                                                                      func.date_add(start=func.col('order_estimated_delivery_date'), days=2)).
                                                          when((func.col('is_success') == 1) & (func.col('order_delivered_customer_date').isNull()),
                                                               func.date_add(start=func.col('order_delivered_carrier_date'), days=2)).
                                                          otherwise(func.col('order_delivered_customer_date'))).drop('order_delivered_customer_date')\
    .na.fill('Missing', subset=['seller_city', 'seller_state', 'customer_city', 'customer_state', 'product_category_name'])

# adding column delivery_delay to see how many days did spent until the customer recieved the package
# adding column is_delayed to see was the package delayed or no
delivery_delay_df = order_delivered_date_fix.\
    withColumn('days_passed_for_delivery',
               func.datediff(func.col('orders_delivered_customer_date'), func.col('order_purchase_timestamp'))).\
    withColumn('delivery_delayed', func.datediff(func.col('orders_delivered_customer_date'), func.col('order_estimated_delivery_date')))\
    .withColumn('is_delayed', func.when(func.col('delivery_delayed') > 0, 'True').otherwise('False'))

# *********************************************************************************
#                           AGGREGATED DATAFRAME
# *********************************************************************************

staged_city_df = order_delivered_date_fix.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).withColumn('iso_city', func.concat(func.lit("BR-"), func.upper(func.col('customer_state'))))

city_df = staged_city_df.groupBy(func.window("order_purchase_timestamp", "5 minutes"), 'iso_state', 'customer_city').\
    agg(func.approx_count_distinct('order_id').alias('most_demanding_city'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("customer_city")))

order_status_df = parased_df.withWatermark("order_purchase_timestamp", "10 minutes") \
    .groupBy(func.window("order_purchase_timestamp", "5 minutes"), func.col("order_status")).\
    agg(func.count('*').alias('Total_Status'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("order_status")))

revenue_per_category_df = payment_type_fix_df.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).\
    groupBy(func.window("order_purchase_timestamp", "5 minutes"),
            payment_type_fix_df.product_category_name).agg(func.round(func.sum('payment_values'), 3)
                                                           .alias('total_revenue_per_category'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("product_category_name")))

revenue_per_product_df = payment_type_fix_df.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).\
    groupBy(func.window("order_purchase_timestamp", "5 minutes"),
            payment_type_fix_df.product_id).agg(func.round(func.sum('payment_values'), 3)
                                                .alias('total_revenue_per_product')).\
    withColumn("composite_id", func.concat(
        func.col("window.start"), func.lit("_"), func.col("product_id")))

seller_revenue_df = delivery_delay_df.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).\
    groupBy(func.window("order_purchase_timestamp", "5 minutes"), 'seller_id').\
    agg(func.round(func.sum('payment_values'), 3).alias('total_revenue_per_seller'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("seller_id")))


royal_customers_df = delivery_delay_df.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).\
    groupBy(func.window("order_purchase_timestamp", "5 minutes"), 'customer_id').\
    agg(func.round(func.sum('payment_values'), 3).alias(
        'total_revenue_per_customer'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("customer_id")))

payment_method_df = delivery_time_df.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).\
    groupBy(func.window("order_purchase_timestamp", "5 minutes"), 'payment_type').\
    agg(func.approx_count_distinct('order_id').alias('total_payment_method'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("payment_type")))

# Adding Insight: Total Revenue Per Order
total_price_per_order_df = payment_type_fix_df.withWatermark("order_purchase_timestamp", "10 minutes")\
    .where(func.col('is_success') == 1).groupBy(func.window("order_purchase_timestamp", "5 minutes"), 'order_id').\
    agg(func.round(func.sum('payment_values'), 3).alias('order_revenue'))\
    .withColumn("composite_id", func.concat(func.col("window.start"), func.lit("_"), func.col("order_id")))

# ******************************************************************
#             WRITING STREAM TO ELASTICSEARCH
# ******************************************************************

category_query = revenue_per_category_df.writeStream\
    .queryName("RevenuePerCategort")\
    .format("org.elasticsearch.spark.sql")\
    .option("es.resource", "revenue_category_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("checkpointLocation", f"{PATH}/temp/revenue")\
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

order_status_query = order_status_df.writeStream\
    .queryName("OrderStatusQuery")\
    .format("org.elasticsearch.spark.sql")\
    .option("es.resource", "order_status_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("checkpointLocation", f"{PATH}/temp/order_statue")\
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

product_query = revenue_per_product_df.writeStream\
    .queryName("RevenuePerProduct")\
    .format("org.elasticsearch.spark.sql")\
    .option("es.resource", "revenue_product_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("checkpointLocation", f"{PATH}/temp/revenue_per_product")\
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

seller_query = seller_revenue_df.writeStream\
    .queryName("RevenuePerSeller")\
    .format("org.elasticsearch.spark.sql")\
    .option("es.resource", "revenue_seller_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("checkpointLocation", f"{PATH}/temp/revenue_per_seller")\
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

customer_query = royal_customers_df.writeStream\
    .queryName("RoyalCustomer")\
    .format("org.elasticsearch.spark.sql")\
    .option("es.resource", "customer_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("checkpointLocation", f"{PATH}/temp/customer")\
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

payment_query = payment_method_df.writeStream\
    .queryName("Payments")\
    .format("org.elasticsearch.spark.sql")\
    .option("checkpointLocation", f"{PATH}/temp/payments")\
    .option("es.resource", "payment_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

city_query = city_df.writeStream\
    .queryName("city")\
    .format("org.elasticsearch.spark.sql")\
    .option("checkpointLocation", f"{PATH}/temp/city")\
    .option("es.resource", "city_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

total_price_query = total_price_per_order_df.writeStream\
    .queryName("total_price")\
    .format("org.elasticsearch.spark.sql")\
    .option("es.resource", "total_price_per_order_index") \
    .option("es.nodes", ELASTIC_HOST) \
    .option("es.port", ELASTIC_PORT) \
    .option("checkpointLocation", f"{PATH}/temp/total_price_per_order")\
    .option("es.mapping.id", "composite_id")\
    .option("es.write.operation", "upsert") \
    .outputMode("update")\
    .trigger(processingTime="90 seconds") \
    .start()

spark_stream_session.streams.awaitAnyTermination()

# export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# export PATH=$JAVA_HOME/bin:$PATH
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0 ~/kafka_elastic.py
