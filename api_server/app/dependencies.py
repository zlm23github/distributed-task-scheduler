import os
from dotenv import load_dotenv
from fastapi import Depends
from services import TaskService

# Load environment variables from .env file
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

_task_service: TaskService = None

async def get_task_service() -> TaskService:
    """Get TaskService instance for dependency injection"""
    global _task_service
    if _task_service is None:
        _task_service = TaskService(
            RABBITMQ_URL,
            REDIS_URL
        )
        await _task_service.connect()
    return _task_service


async def close_task_service():
    """Close the TaskService instance"""
    global _task_service
    if _task_service:
        await _task_service.close()
        _task_service = None


TaskServiceDep = Depends(get_task_service)