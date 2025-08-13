from kafka import KafkaConsumer
import json
import requests
import os

#HTTP_NEW_URL = "https://testfor4glte.vercel.app/api/voltages/new"
HTTP_NEW_URL = "http://localhost:3000/api/voltages/new"
LAST_TS_FILE = "last_fetched.txt"

def get_last_timestamp():
    if os.path.exists(LAST_TS_FILE):
        with open(LAST_TS_FILE, "r") as f:
            return f.read().strip()
    return "1970-01-01T00:00:00Z"

def set_last_timestamp(ts):
    with open(LAST_TS_FILE, "w") as f:
        f.write(ts)

def poll_new_data():
    since = get_last_timestamp()
    try:
        response = requests.get(HTTP_NEW_URL, params={"since": since}, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"[INFO] New records: {data}")
                # Update last timestamp to latest record
                latest_ts = max([d["timestamp"] for d in data])
                set_last_timestamp(latest_ts)
            else:
                print("[INFO] No new records found.")
        else:
            print(f"[WARN] Poll failed: HTTP {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Polling error: {e}")

def start_kafka_listener():
    consumer = KafkaConsumer(
        'control-commands',
        bootstrap_servers=['localhost:9092'],  # Same as Express producer
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='pi-4g-listener',
        auto_offset_reset='latest'
    )
    print("[INFO] Listening for Kafka events...")
    for message in consumer:
        print(f"[EVENT] Kafka message: {message.value}")
        if message.value.get("event") == "new_data":
            poll_new_data()

if __name__ == "__main__":
    start_kafka_listener()
