import os
import json
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "lakehouse", "silver")
BRONZE_DIR = os.path.join(BASE_DIR, "lakehouse", "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)


os.makedirs(SILVER_DIR, exist_ok=True)

consumer = KafkaConsumer(
    "glucose_raw",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

def parse_bool_tags(tags):
    if tags is None:
        t = ""
    else:
        # handle pandas NaN (float)
        try:
            if pd.isna(tags):
                t = ""
            else:
                t = str(tags)
        except:
            t = str(tags)

    t = t.lower()

    is_fasting = "fasting" in t
    is_post_meal = "after meal" in t or "post" in t

    meal_type = None
    for m in ["breakfast", "lunch", "dinner", "snack"]:
        if m in t:
            meal_type = m
            break

    return is_fasting, is_post_meal, meal_type


buffer = []
BATCH_SIZE = 25  # write every 25 events

print("Streaming: Kafka -> Silver (Parquet). Press Ctrl+C to stop.")

try:
    for msg in consumer:
        e = msg.value

        # Write raw event to Bronze as JSONL (append-only)
        bronze_date = str(e.get("Date", "")).split(" ")[0] or "unknown"
        bronze_path = os.path.join(BRONZE_DIR, f"event_date={bronze_date}")
        os.makedirs(bronze_path, exist_ok=True)

        with open(os.path.join(bronze_path, "events.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(e, default=str) + "\n")

        date_str = str(e.get("Date", "")).strip()
        time_str = str(e.get("Time", "")).strip()
        tags = e.get("Tags", None)
        glucose = e.get("Blood Sugar Measurement (mg/dL)", None)
        food_type = e.get("Food type", None)
        timezone = e.get("Timezone", None)

        event_ts = None
        if date_str and time_str:
            date_only = date_str.split(" ")[0]
            event_ts = f"{date_only} {time_str}"

        is_fasting, is_post_meal, meal_type = parse_bool_tags(tags)

        row = {
            "event_ts": event_ts,
            "glucose_mg_dl": glucose,
            "tags": tags,
            "is_fasting": is_fasting,
            "is_post_meal": is_post_meal,
            "meal_type": meal_type,
            "food_type": food_type,
            "timezone": timezone,
            "ingested_at": datetime.utcnow().isoformat(),
        }

        buffer.append(row)
        print("Processed:", row)

        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)

            df["event_date"] = df["event_ts"].astype(str).str.slice(0, 10)
            for d, part in df.groupby("event_date"):
                out_path = os.path.join(SILVER_DIR, f"event_date={d}")
                os.makedirs(out_path, exist_ok=True)
                file_name = f"silver_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
                part.drop(columns=["event_date"]).to_parquet(os.path.join(out_path, file_name), index=False)

            print(f"Wrote {len(buffer)} rows to Silver.")
            buffer = []

except KeyboardInterrupt:
    print("Stopping stream processor...")
