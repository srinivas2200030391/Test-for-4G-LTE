from kafka import KafkaConsumer
import json
import requests
import os
from googletrans import Translator
from gtts import gTTS
from playsound import playsound

#HTTP_NEW_URL = "http://localhost:3000/api/messages/new"
HTTP_NEW_URL = "https://testfor4glte.vercel.app/api/messages/new"
LAST_TS_FILE = "last_fetched.txt"

translator = Translator()

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

def translate_text(text, dest_lang="te"):
    """
    Translate text to Telugu by default.
    """
    try:
        translated = translator.translate(text, dest=dest_lang)
        return translated.text
    except Exception as e:
        print(f"[ERROR] Translation failed: {e}")
        return text

def speak_telugu(text, slow=True):
    """
    Convert Telugu text into speech using Google TTS.
    """
    try:
        tts = gTTS(text=text, lang="hi", slow=slow)  # slow=True = gentle pace 💕
        filename = "temp_te.mp3"
        tts.save(filename)
        playsound(filename)
        os.remove(filename)
    except Exception as e:
        print(f"[ERROR] Telugu speech failed: {e}")

def poll_new_data_and_speak():
    since = get_last_timestamp()
    try:
        response = requests.get(HTTP_NEW_URL, params={"since": since}, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"[INFO] New records: {data}")
                latest_ts = max([d["timestamp"] for d in data])
                set_last_timestamp(latest_ts)

                # Extract message
                tts_text = data[0].get("message", "No message found")

                # Translate to Telugu 🌸
                tts_text = translate_text(tts_text, "hi")
                print(isinstance(tts_text, str), "tts_text:", tts_text)

                # Speak in Telugu slowly 🎶
                speak_telugu(tts_text, slow=True)

            else:
                print("[INFO] No new records found.")
        else:
            print(f"[WARN] Poll failed: HTTP {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Polling error: {e}")

def start_kafka_listener():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # This points to /api
    certs_dir = os.path.join(BASE_DIR, "api/certs")
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
        elif message.value.get("event") == "new_message":
            poll_new_data_and_speak()

if __name__ == "__main__":
    start_kafka_listener()
