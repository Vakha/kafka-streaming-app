from datetime import datetime
from time import sleep
import json
from kafka import KafkaProducer

DATA_DIR = "/Users/alex/study/try_structured_streaming/data"

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    key_serializer=lambda x: str(x).encode("utf-8"),
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

with open(f"{DATA_DIR}/normal_msg_sequence.json", "r") as file:
    for line in file:
        msg = json.loads(line)
        # msg["timestamp"] = datetime.now().astimezone().isoformat()
        producer.send("output-topic-6", value=msg)
        print(f"Sent: {msg}")
        sleep(4)
