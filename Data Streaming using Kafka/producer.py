import json
from confluent_kafka import Producer
import pandas as pd
import time

import os
print("Current working directory:", os.getcwd())

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to topic: {msg.topic()} | partition: {msg.partition()} | offset: {msg.offset()}")
        print(f"Message: {msg.value().decode('utf-8')}\n")

orders = pd.read_csv("Olist Dataset/olist_orders_dataset.csv")
order_items = pd.read_csv("Olist Dataset/olist_order_items_dataset.csv")
order_payments = pd.read_csv("Olist Dataset/olist_order_payments_dataset.csv")

for _, row in orders.iterrows():
    message = json.dumps(row.to_dict()).encode("utf-8")
    producer.produce(topic="orders_topic", value=message, callback=delivery_report)
    producer.poll(0)  # Handle delivery callbacks promptly
    time.sleep(1)

for _, row in order_items.iterrows():
    message = json.dumps(row.to_dict()).encode("utf-8")
    producer.produce(topic="order_items_topic", value=message, callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

for _, row in order_payments.iterrows():
    message = json.dumps(row.to_dict()).encode("utf-8")
    producer.produce(topic="order_payments_topic", value=message, callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

producer.flush()

print("✅ All messages sent successfully!")
