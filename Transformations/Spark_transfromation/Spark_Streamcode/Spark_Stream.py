from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import from_json
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, FloatType, DateType, TimestampType, DoubleType

spark_stream_session = SparkSession.builder.appName(
    'Spark_Consume').master('local').getOrCreate()

spark_stream_session._jsc.hadoopConfiguration().set(
    "fs.s3a.access.key", "ACCESS_KEY")  # access key
spark_stream_session._jsc.hadoopConfiguration().set(
    "fs.s3a.secret.key", "SECRET_KEY")  # secret key
spark_stream_session._jsc.hadoopConfiguration().set(
    "fs.s3a.endpoint", "s3.amazonaws.com")  # url

streamed_df = spark_stream_session.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', "localhost:9092")\
    .option('subscribe', 'Store_Topic')\
    .option('startingOffesets', 'earliest')\
    .load()
streamed_df = streamed_df.select(
    func.col('key').cast('string'),
    func.col('value').cast('string')
)
schema = StructType([StructField('index', IntegerType(), True), StructField('order_id', StringType(), True),
                     StructField('customer_id', StringType(), True), StructField(
                         'order_status', StringType(), True),
                     StructField('order_purchase_timestamp',
                                 TimestampType(), True),
                     StructField('order_approved_at', TimestampType(), True),
                     StructField('order_delivered_carrier_date',
                                 TimestampType(), True),
                     StructField('order_delivered_customer_date',
                                 TimestampType(), True),
                     StructField('order_estimated_delivery_date',
                                 TimestampType(), True),
                     StructField('order_item_id', DoubleType(), True),
                     StructField('product_id', StringType(), True),
                     StructField('seller_id', StringType(), True),
                     StructField('seller_limit_date_to_ship_to_vendor',
                                 TimestampType(), True),
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
                     StructField('seller_state', StringType(), True)])

parased_df = streamed_df.select(from_json(func.col('value'), schema=schema).alias('data'))\
    .select('data.*')


delivery_time_df = parased_df.withColumn('is_success',
                                         func.when(func.lower(func.col('order_status')).isin("delivered", "shipped", "invoiced"), 1).otherwise(0)).\
    na.fill('not_defined', 'payment_type')

payment_type_fix_df = delivery_time_df.withColumn('payment_values',
                                                  func.when(func.col('is_success') == 0, 0).
                                                  when(func.col('payment_value').isNull() &
                                                       (func.col('is_success') == 1), func.col(
                                                           'price') + func.col('freight_value')
                                                       ).otherwise(func.col('payment_value'))
                                                  ).drop(func.col('payment_value'))

# fix nulls in order_delivered_customer_date if is_success = 1 & date = null then date = carrierdate + 2 days
order_delivered_date_fix = payment_type_fix_df.withColumn('orders_delivered_customer_date', func.when((func.col("is_success") == 1) &
                                                                                                      (func.col('order_delivered_customer_date').isNull()) &
                                                                                                      (func.col(
                                                                                                          'order_delivered_carrier_date').isNull()),
                                                                                                      func.date_add(start=func.col('order_estimated_delivery_date'), days=2)).
                                                          when((func.col("is_success") == 1) & (func.col('order_delivered_customer_date').isNull()),
                                                               func.date_add(start=func.col('order_delivered_carrier_date'), days=2)).
                                                          otherwise(func.col('order_delivered_customer_date'))).drop('order_delivered_customer_date')\
    .na.fill('Missing', subset=['seller_city', 'seller_state', 'customer_city', 'customer_state', 'product_category_name'])

# adding column delivery_delay to see how many days did spent until the customer recieved the package
# adding column is_delayed to see was the package delayed or no
delivery_delay_df = order_delivered_date_fix.\
    withColumn('days_passed_for_delivery',
               func.timestamp_diff('Day', func.col('order_purchase_timestamp'), func.col('orders_delivered_customer_date'))).\
    withColumn('delivery_delayed', func.date_diff(func.col('orders_delivered_customer_date'), func.col('order_estimated_delivery_date')))\
    .withColumn('is_delayed', func.when(func.col('delivery_delayed') > 0, 'True').otherwise('False'))

# adding dates and time to order_purchase for more insights
dates_df = delivery_delay_df.\
    withColumn('order_purchase_hour', func.hour('order_purchase_timestamp')).\
    withColumn('order_purchase_dayname', func.dayname('order_purchase_timestamp')).\
    withColumn('order_purchase_day', func.day('order_purchase_timestamp')).\
    withColumn('order_purchase_month', func.month('order_purchase_timestamp')).\
    withColumn('order_purchase_monthname', func.monthname('order_purchase_timestamp')).\
    withColumn('order_purchase_year', func.year('order_purchase_timestamp'))

# count of order status
order_status_df = parased_df.groupBy(parased_df.order_status).agg(func.count('*').alias('Total_Status')).\
    sort('Total_Status', ascending=False)

revenue_per_category_df = payment_type_fix_df.where(func.col('is_success') == 1).\
    groupBy(payment_type_fix_df.product_category_name).agg(func.round(func.sum('payment_values'), 3)
                                                           .alias('total_revenue_per_category')).orderBy('total_revenue_per_category', ascending=False)

revenue_per_product_df = payment_type_fix_df.where(func.col('is_success') == 1).\
    groupBy(payment_type_fix_df.product_id).agg(func.round(func.sum('payment_values'), 3)
                                                .alias('total_revenue_per_product')).orderBy('total_revenue_per_product', ascending=False)

seller_revenue_df = delivery_delay_df.where(func.col('is_success') == 1).\
    groupBy('seller_id').\
    agg(func.round(func.sum('payment_values'), 3).alias('total_revenue_per_seller')).\
    orderBy('total_revenue_per_seller', ascending=False)

royal_customers_df = delivery_delay_df.where(func.col('is_success') == 1).\
    groupBy('customer_id').\
    agg(func.round(func.sum('payment_values'), 3).alias('total_revenue_per_customer')).\
    orderBy('total_revenue_per_customer', ascending=False)

payment_method_df = delivery_time_df.where(func.col('is_success') == 1).\
    groupBy('payment_type').agg(func.countDistinct('order_id').alias('total_payment_method')).\
    orderBy('total_payment_method', ascending=False)

city_df = order_delivered_date_fix.where(func.col('is_success') == 1).\
    groupBy('customer_city').agg(func.countDistinct('order_id').alias('most_demanding_city')).\
    orderBy('most_demanding_city', ascending=False)

# Adding Insight: Total Revenue Per Order
total_price_per_order_df = payment_type_fix_df.where(func.col('is_success') == 1).groupBy('order_id').\
    agg(func.round(func.sum('payment_values'), 3).alias('order_revenue')).\
    orderBy('order_revenue', ascending=False)


query = dates_df.writeStream.outputMode('append').format('console').start()
# query = dates_df.writeStream \
#     .format("parquet") \
#     .option("path", "s3a://your-bucket-name/folder/") \
#     .option("checkpointLocation", "s3a://your-bucket-name/checkpoint/")\
#     .outputMode("append") \
#     .start()
# checkpointlocation to save the last saved point
query.awaitTermination()
