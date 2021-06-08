from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    key_serializer=lambda x: str(x).encode('utf-8'),
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

for e in range(100):
    data = {'number' : e}
    producer.send('output-topic', key=e, value=data)
    print(f'Sent: {e} : {data}')
    sleep(1)
