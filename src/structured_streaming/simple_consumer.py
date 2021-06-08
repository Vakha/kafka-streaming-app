from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'output-topic',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     key_deserializer=lambda x: int(x.decode('utf-8')),
     value_deserializer=lambda x: loads(x.decode('utf-8')),
)


for message in consumer:
    print(f'Received: {message.key} : {message.value}')