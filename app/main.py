# app/main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.producer import publish_user_event
from app.tasks import celery_app

app = FastAPI()

class User(BaseModel):
    user_id: int
    email: str
    name: str

@app.post("/register")
async def register_user(user: User):
    try:
        # 1. Publish event to Kafka
        publish_user_event(user.dict())
        return {"status": "success", "message": "User registered", "user": user}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    task = celery_app.AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status": task.status,
        "result": task.result if task.successful() else None
    }