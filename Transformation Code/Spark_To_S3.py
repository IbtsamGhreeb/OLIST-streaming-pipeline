# ******************************************************************
#               IMPORTING LIBERERIES
# ******************************************************************

from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import from_json
from pyspark.sql.types import IntegerType, StructField, StructType, DoubleType, TimestampType, StringType

# ******************************************************************
#                 SETTING VARIABLES
# ******************************************************************

ICEBERG_WAREHOUSE = 's3a://<Bucket_Name>/Path'
S3_ENDPOINT = "s3.<location>.amazonaws.com"
S3_ACCESS_KEY = "S3_Access_Key"
S3_SECRET_KEY = "S3_Secret_Key"

# ******************************************************************
#                 STARTING SPARK SESSION
# ******************************************************************

spark_stream_session = SparkSession.builder.appName(
    'Spark_Consume').master('local')\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.catalog.brazilian_catalog", "org.apache.iceberg.spark.SparkCatalog")\
    .config("spark.sql.catalog.brazilian_catalog.type", "hadoop")\
    .config("spark.sql.catalog.brazilian_catalog.warehouse", ICEBERG_WAREHOUSE)\
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
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
schema = StructType([
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

# ******************************************************************
#                   SPARK TRANSFORMATIONS
# ******************************************************************

parased_df = streamed_df.select(from_json(func.col('value'), schema=schema).alias('data'))\
    .select('data.*')

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

# fix nulls in order_delivered_customer_date if is_success = 1 & date = null then date = carrierdate + 12 days
order_delivered_date_fix = payment_type_fix_df.withColumn('orders_delivered_customer_date', func.when((func.col('is_success') == 1) &
                                                                                                      (func.col('order_delivered_customer_date').isNull()) &
                                                                                                      (func.col(
                                                                                                          'order_delivered_carrier_date').isNull()),
                                                                                                      func.date_add(start=func.col('order_estimated_delivery_date'), days=12)).
                                                          when((func.col('is_success') == 1) & (func.col('order_delivered_customer_date').isNull()),
                                                               func.date_add(start=func.col('order_delivered_carrier_date'), days=12)).
                                                          otherwise(func.col('order_delivered_customer_date'))).drop('order_delivered_customer_date')\
    .na.fill('Missing', subset=['seller_city', 'seller_state', 'customer_city', 'customer_state', 'product_category_name'])

# adding column delivery_delay to see how many days did spent until the customer recieved the package
# adding column is_delayed to see was the package delayed or no
delivery_delay_df = order_delivered_date_fix.\
    withColumn('days_passed_for_delivery',
               func.datediff(func.col('orders_delivered_customer_date'), func.col('order_purchase_timestamp'))).\
    withColumn('delivery_delayed', func.datediff(func.col('orders_delivered_customer_date'), func.col('order_estimated_delivery_date')))\
    .withColumn('is_delayed', func.when(func.col('delivery_delayed') > 0, 'True').otherwise('False'))

# adding dates and time to order_purchase for more insights
dates_df = delivery_delay_df.\
    withColumn('order_purchase_hour', func.hour('order_purchase_timestamp')).\
    withColumn('order_purchase_dayname', func.date_format('order_purchase_timestamp', 'EEEE')).\
    withColumn('order_purchase_day', func.dayofmonth('order_purchase_timestamp')).\
    withColumn('order_purchase_month', func.month('order_purchase_timestamp')).\
    withColumn('order_purchase_monthname', func.date_format('order_purchase_timestamp', 'MMMM')).\
    withColumn('order_purchase_year', func.year('order_purchase_timestamp'))

# *********************************************************************************
#                               ICEBERG CATALOG
# *********************************************************************************

spark_stream_session.sql("""
CREATE TABLE IF NOT EXISTS brazilian_catalog.olist.iceberg_config(
            index INTEGER,
            order_id STRING,
            customer_id STRING,
            order_status STRING,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            order_item_id DOUBLE,
            product_id STRING,
            seller_id STRING,
            seller_limit_date_to_ship_to_vendor TIMESTAMP,
            price DOUBLE,
            freight_value DOUBLE,
            customer_unique_id STRING,
            customer_city STRING,
            customer_state STRING,
            payment_sequential DOUBLE,
            payment_type STRING,
            payment_installments DOUBLE,
            product_category_name STRING,
            seller_city STRING,
            seller_state STRING,
            is_success INTEGER,
            payment_values DOUBLE,
            orders_delivered_customer_date TIMESTAMP,
            days_passed_for_delivery BIGINT,
            delivery_delayed INTEGER,
            is_delayed STRING,
            order_purchase_hour INTEGER,
            order_purchase_dayname STRING,
            order_purchase_day INTEGER,
            order_purchase_month INTEGER,
            order_purchase_monthname STRING,
            order_purchase_year INTEGER)
            USING iceberg
            PARTITIONED BY (order_purchase_year)
""")

# ******************************************************************
#             WRITING STREAM TO S3 BUCKET
# ******************************************************************

query = dates_df.writeStream \
    .queryName('Brazilian_Stream')\
    .format('iceberg') \
    .option('checkpointLocation', f'{ICEBERG_WAREHOUSE}/Checkpoints/Brazilian_Checkpoint')\
    .outputMode('append') \
    .option('maxOffsetsPerTrigger', 1000)\
    .trigger(processingTime='30 seconds')\
    .toTable("brazilian_catalog.olist.iceberg_config")
# checkpointlocation to save the last saved point

spark_stream_session.streams.awaitAnyTermination()

# spark-submit \
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.607 \
# ~/s3_spark.py
