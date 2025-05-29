# app/consumer_email.py

from kafka import KafkaConsumer
from app.tasks import send_welcome_email
import json

TOPIC_NAME = "user.events"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    group_id="email-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("📥 Email consumer listening...")

for message in consumer:
    event = message.value
    print(f"📬 Received event in email consumer: {event}")
    task = send_welcome_email.delay(event['email'], event['name'])
    print(f"Celery task ID: {task.id}")
