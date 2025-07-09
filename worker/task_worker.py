import logging
import asyncio
import aio_pika
import json
import redis.asyncio as redis
import os

from worker.task_handlers import TaskHandlerFactory
from datetime import datetime

logger = logging.getLogger(__name__)

class TaskWorker:
    def __init__(self, connection: aio_pika.Connection, channel: aio_pika.Channel, redis_client: redis.Redis):
        """
        Initialize TaskWorker with established connections.
        
        Args:
            connection: Established RabbitMQ connection
            channel: Established RabbitMQ channel
            redis_client: Established Redis client
        """
        self.connection = connection
        self.channel = channel
        self.redis_client = redis_client
        
        # Set QoS for fair distribution
        asyncio.create_task(self.channel.set_qos(prefetch_count=10))
        
        logger.info("TaskWorker initialized with established connections")

    async def close(self):
        """Close connections (called by the parent worker)"""
        if self.connection:
            await self.connection.close()
        if self.redis_client:
            await self.redis_client.close()
        logger.info("TaskWorker connections closed")

    async def update_task_status(self, task_id: str, status: str, result=None, error=None):
        """Update task status in Redis"""
        try:
            task_key = f"task:{task_id}"
            task_data = await self.redis_client.get(task_key)
            
            if not task_data:
                logger.warning(f"Task {task_id} not found in Redis")
                return None

            task = json.loads(task_data)
            task["status"] = status
            
            if status == "running":
                task["started_at"] = datetime.utcnow().isoformat()
            elif status in ["completed", "failed"]:
                task["completed_at"] = datetime.utcnow().isoformat()
            
            if result is not None:
                task["result"] = result
            if error is not None:
                task["error"] = error

            # Write back to Redis
            await self.redis_client.set(task_key, json.dumps(task), ex=3600)
            logger.info(f"Task {task_id} status updated to: {status}")
            return task
        
        except Exception as e:
            logger.error(f"Failed to update task status for {task_id}: {e}")
            raise

    async def process_task(self, task_data: dict):
        """Process a single task"""
        try:
            task_id = task_data.get("task_id")
            task_type = task_data.get("task_type")
            parameters = task_data.get("parameters")

            print(f"⚙️  [Worker] Processing task {task_id} with type: {task_type} ...")
            logger.info(f"⚙️  [Worker] Processing task {task_id} with type: {task_type} ...")

            handler = TaskHandlerFactory.get_handler(task_type)
            result = await handler.handle(parameters)

            # Update task status
            await self.update_task_status(task_id, "completed", result=result, error=None)

            print(f"🎯 [Worker] Task {task_id} completed with result: {result}")
            logger.info(f"🎯 [Worker] Task {task_id} completed with result: {result}")

        except Exception as e:
            print(f"💥 [Worker] Error processing task {task_id}: {e}")
            logger.error(f"💥 [Worker] Error processing task {task_id}: {e}")
            raise
    
    async def handle_message(self, message):
        """Handle incoming message from RabbitMQ"""
        try:
            async with message.process():
                body = message.body.decode('utf-8')
                task_data = json.loads(body)
                
                task_id = task_data.get("task_id")
                task_type = task_data.get("task_type")
                
                # Add message consumption output
                print(f"🔄 [Worker] Consuming task: {task_id} (type: {task_type})")
                logger.info(f"🔄 [Worker] Consuming task: {task_id} (type: {task_type})")
                
                # Update task status to running
                await self.update_task_status(task_id, "running")

                # Process task  
                await self.process_task(task_data)

                print(f"✅ [Worker] Task {task_id} processed successfully")
                logger.info(f"✅ [Worker] Task {task_id} processed successfully")

        except Exception as e:
            print(f"❌ [Worker] Error processing message: {e}")
            logger.error(f"❌ [Worker] Error processing message: {e}")
            raise

    async def process_tasks(self):
        """Start processing tasks from the queue"""
        try:
            # Declare queue
            queue = await self.channel.declare_queue("task_queue", durable=True)
            
            # Start consuming messages
            await queue.consume(self.handle_message)

            logger.info("TaskWorker started, waiting for tasks...")

            # Run forever
            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in TaskWorker: {e}")
            raise