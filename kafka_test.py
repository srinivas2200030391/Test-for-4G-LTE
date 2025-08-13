from kafka import KafkaConsumer
import json
import requests
import os

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
                latest_ts = max([d["timestamp"] for d in data])
                set_last_timestamp(latest_ts)
            else:
                print("[INFO] No new records found.")
        else:
            print(f"[WARN] Poll failed: HTTP {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Polling error: {e}")

def start_kafka_listener():
    # Get current file's directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Go up one level and into "certs"
    certs_dir = os.path.join(current_dir, ".", "certs")

    # Normalize the path so ".." is resolved
    certs_dir = os.path.normpath(certs_dir)
    print(certs_dir)
    consumer = KafkaConsumer(
        "control-commands",
        bootstrap_servers=["kafka-36cdd7ab-cronack-2088.e.aivencloud.com:19352"],
        security_protocol="SSL",
        ssl_cafile=os.path.join(certs_dir, "ca.pem"),
        ssl_certfile=os.path.join(certs_dir, "service.cert"),
        ssl_keyfile=os.path.join(certs_dir, "service.key"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    print("[INFO] Listening for Kafka events...")
    for message in consumer:
        if not message.value:
            continue
        print(f"[EVENT] Kafka message: {message.value}")
        if message.value.get("event") == "new_data":
            poll_new_data()

if __name__ == "__main__":
    start_kafka_listener()
