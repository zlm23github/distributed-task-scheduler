import uuid
import json
import redis.asyncio as redis
import aio_pika
from datetime import datetime
from typing import Optional, List

from models import TaskCreate, TaskResponse, TaskUpdate, TaskStatus

class TaskService:
    def __init__(self, rabbitmq_url: str, redis_url: str):
        print("==> [services.py] Initializing TaskService")

        self.rabbitmq_url = rabbitmq_url
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None

        self.redis_url = redis_url
        self.redis_client = None


    async def connect(self):
        """initialize the connection to rabbitmq and redis"""
        try:
            # connect to rabbitmq
            print(f"==> [services.py] Connecting to RabbitMQ: {self.rabbitmq_url}")
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            print("Connected to RabbitMQ")
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            print("Created RabbitMQ channel")

            #queue for task
            queue = await self.rabbitmq_channel.declare_queue("task_queue", durable=True)
            print(f"Declared queue: {queue.name}")

            # connect to redis
            self.redis_client = redis.from_url(self.redis_url)
            print("Connected to Redis")

        except Exception as e:
            print(f"Error connecting to RabbitMQ or Redis: {e}")
            raise


    async def submit_task(self, task: TaskCreate) -> TaskResponse:
        """create a new task"""
        try:
            # generate a unique task id
            task_id = str(uuid.uuid4())

            task_response = TaskResponse(
                task_id=task_id,
                task_type=task.task_type,
                status=TaskStatus.PENDING,
                parameters=task.parameters,
                priority=task.priority,
                created_at=datetime.utcnow(),
            )

            await self._save_task_to_redis(task_response)
            await self._publish_task(task_response)

            return task_response

        except Exception as e:
            print(f"Error sending task: {e}")
            raise

    async def get_task_by_id(self, task_id: str) -> Optional[TaskResponse]:
        """get a task by id"""
        try:
            task_data = await self.redis_client.get(f"task:{task_id}")

            if not task_data:
                return None

            task_dict = json.loads(task_data)
            task_response = TaskResponse(**task_dict)

            return task_response

        except Exception as e:
            print(f"Error getting task from Redis: {e}")
            raise Exception(f"Error getting task from Redis: {e}")


    async def get_all_tasks(self) -> List[TaskResponse]:
        """get all tasks"""
        try:
            tasks = []
            async for key in self.redis_client.scan_iter("task:*"):
                task_data = await self.redis_client.get(key)
                if task_data:
                    task_dict = json.loads(task_data)
                    task_response = TaskResponse(**task_dict)
                    tasks.append(task_response)

            print(f"Found {len(tasks)} tasks in Redis")
            return tasks

        except Exception as e:
            print(f"Error getting all tasks from Redis: {e}")
            raise Exception(f"Error getting all tasks from Redis: {e}")


    async def update_task_status(self, task_id: str, task_update: TaskUpdate):
        """update the status of a task"""
        try:
            task = await self.get_task_by_id(task_id)
            if not task:
                return None

            task.status = task_update.status
            task.result = task_update.result
            task.error = task_update.error

            await self._save_task_to_redis(task)

            print(f"Task {task_id} updated in Redis")
            return task
        
        except Exception as e:
            print(f"Error updating task {task_id} in Redis: {e}")
            raise Exception(f"Error updating task {task_id} in Redis: {e}")


    async def delete_task(self, task_id: str) -> bool:
        """delete a task"""
        try:
            task = await self.get_task_by_id(task_id)
            if not task:
                return False

            await self.redis_client.delete(f"task:{task_id}")
            print(f"Task {task_id} deleted from Redis")

            return True

        except Exception as e:
            print(f"Error deleting task {task_id} from Redis: {e}")
            raise Exception(f"Error deleting task {task_id} from Redis: {e}")


    async def _save_task_to_redis(self, task: TaskResponse):
        """save the task to redis"""
        try:
            task_json = task.json()

            await self.redis_client.set(f"task:{task.task_id}", task_json, ex=3600)
            
            print(f"Task {task.task_id} saved to Redis")

        except Exception as e:
            print(f"Error saving task to Redis: {e}")
            raise Exception(f"Error saving task to Redis: {e}")


    async def _publish_task(self, task: TaskResponse):
        """publish the task to rabbitmq"""
        try:
            task_json = task.json()

            # create RabbitMQ message
            message = aio_pika.Message(
                body=task_json.encode('utf-8'),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type='application/json'
            )

            # publish the message to the queue
            await self.rabbitmq_channel.default_exchange.publish(
                message, routing_key="task_queue"
            )

            print(f"Task {task.task_id} published to RabbitMQ")

        except Exception as e:
            print(f"Error publishing task to RabbitMQ: {e}")
            raise Exception(f"Error publishing task to RabbitMQ: {e}")
        

    async def close(self):
        """close the connection to rabbitmq and redis"""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            print("Closed RabbitMQ connection")
        if self.redis_client:
            await self.redis_client.close()
            print("Closed Redis connection")
