from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "glucose_raw",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Listening on topic glucose_raw...")

for msg in consumer:
    print(msg.value)
