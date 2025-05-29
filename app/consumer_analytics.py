# app/consumer_analytics.py

from kafka import KafkaConsumer
import json
import datetime

TOPIC_NAME = "user.events"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    group_id="analytics-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("📊 Analytics consumer listening...")

log_file = "analytics_log.txt"

for message in consumer:
    event = message.value
    timestamp = datetime.datetime.now().isoformat()
    log_line = f"[{timestamp}] New user registered: {event['name']} ({event['email']})\n"
    print(log_line.strip())
    with open(log_file, "a") as f:
        f.write(log_line)
