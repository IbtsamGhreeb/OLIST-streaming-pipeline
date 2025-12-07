from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         client_id='Store_Producer',
                         key_serializer=lambda key: key.encode(
                             'utf-8'),  # same as json.dumps(key)
                         value_serializer=lambda row: json.dumps(
                             row).encode('utf-8')
                         )
if producer.bootstrap_connected():
    print('Producer Connected To Topics')

    with open(r'./Olist_joined_Table_af.csv', 'r', encoding='utf-8') as f:
        float_cols = ['order_item_id', 'price', 'freight_value',
                      'payment_sequential', 'payment_installments', 'payment_value']
        int_cols = ['index']
        reader = csv.DictReader(f)
        for row in reader:
            for col in float_cols:
                value = row.get(col)
                try:
                    row[col] = float(value) if value else None
                except (ValueError, TypeError):
                    row[col] = None

            for col in int_cols:
                value = row.get(col)
                try:
                    row[col] = int(float(value)) if value else None
                except (ValueError, TypeError):
                    row[col] = None

            key = row['product_category_name']
            producer.send('Store_Topic', key=key, value=row)
            print(f'Rows sent ---> {row} to the key ---> {key}')
            time.sleep(1.5)
    producer.flush()
