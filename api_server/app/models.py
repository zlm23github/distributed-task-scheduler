from enum import Enum
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime
import uuid

class TaskType(str, Enum):
    WEB_SCRAPER = "web_scraper"
    CALCULATOR = "calculator"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class TaskCreate(BaseModel):
    task_type: TaskType
    parameters: Dict[str, Any]
    priority: Optional[int] = 1

class TaskResponse(BaseModel):
    task_id: str
    task_type: TaskType
    status: TaskStatus
    parameters: Dict[str, Any]
    priority: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class TaskUpdate(BaseModel):
    status: TaskStatus
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None