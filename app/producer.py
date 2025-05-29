# app/producer.py

from kafka import KafkaProducer
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "user.events"

def publish_user_event(event: dict):
    print(f"Publishing to Kafka: {event}")
    producer.send(TOPIC_NAME, value=event)
    producer.flush()
