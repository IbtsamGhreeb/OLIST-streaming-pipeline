from kafka.admin import KafkaAdminClient, NewTopic

# create topic
client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='store_Api'
)
try:
    topic = NewTopic('Store_Topic', 4, 1)
    client.create_topics([topic])
    print(f'topic {topic.name} was success')
except Exception as e:
    print(f'topic {topic.name} failed|')
    print(f'the error was: {e}')
