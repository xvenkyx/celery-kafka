# app/tasks.py

from celery import Celery
import time

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

@celery_app.task
def send_welcome_email(email, name):
    print(f"📧 Sending welcome email to {name} at {email}...")
    time.sleep(3)  # simulate email sending delay
    return f"Email sent to {email}"
