import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
excel_path = os.path.join(BASE_DIR, "mySugr_Export_2026-02-11-18-32.xlsx")

df = pd.read_excel(excel_path)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)


for _, row in df.iterrows():
    event = row.to_dict()
    producer.send("glucose_raw", event)
    print("Sent:", event)
    time.sleep(0.2)

producer.flush()
print("Data sent to Kafka")
