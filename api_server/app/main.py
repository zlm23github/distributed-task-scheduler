from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import List
import uvicorn

from models import TaskCreate, TaskResponse, TaskUpdate, TaskStatus
from dependencies import get_task_service, close_task_service, TaskServiceDep
from services import TaskService

@asynccontextmanager
async def lifespan(app: FastAPI):
    """lifespan for the app"""
    print("🚀 Starting application initialization...")
    try:
        await get_task_service()
        print("✅ Application initialization completed")
    except Exception as e:
        print(f"❌ Application initialization failed: {e}")
        raise
    yield
    print("🛑 Shutting down application...")
    await close_task_service()
    print("✅ Application shutdown completed")

app = FastAPI(title="Distributed Task API",
    description="A distributed task API for managing and executing tasks",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "API server is running"}

@app.post("/tasks", response_model=TaskResponse)
async def submit_task(task: TaskCreate, task_service: TaskService = TaskServiceDep):
    return await task_service.submit_task(task)


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task_by_id(task_id: str, task_service: TaskService = TaskServiceDep):
    return await task_service.get_task_by_id(task_id)


@app.get("/tasks", response_model=List[TaskResponse])
async def get_all_tasks(task_service: TaskService = TaskServiceDep):
    return await task_service.get_all_tasks()


@app.put("/tasks/{task_id}", response_model=TaskResponse)
async def update_task(task_id: str, task_update: TaskUpdate, task_service: TaskService = TaskServiceDep):
    task = await task_service.update_task_status(task_id, task_update)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found")
    return task


@app.delete("/tasks/{task_id}", response_model=bool)
async def delete_task(task_id: str, task_service: TaskService = TaskServiceDep):
    success = await task_service.delete_task(task_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found")
    return True


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)