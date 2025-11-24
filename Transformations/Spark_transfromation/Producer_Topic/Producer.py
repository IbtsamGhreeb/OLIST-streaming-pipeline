from kafka import KafkaProducer
import csv
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         client_id='Store_Producer',
                         key_serializer=lambda key: key.encode(
                             'utf-8'),  # same as json.dumps(key)
                         value_serializer=lambda row: json.dumps(
                             row).encode('utf-8')
                         )
if producer.bootstrap_connected():
    print('Producer Connected To Topics')
    with open(r'D:/project/olst/pandas transformation/Transformations/pandas_transformation/Olist_joined_Table_af.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row['product_category_name']
            producer.send('Store_Topic', key=key, value=row)
            print(f'Rows sent ---> {row} to the key ---> {key}')
    producer.flush()
