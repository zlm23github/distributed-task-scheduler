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
    def __init__(self, rabbitmq_url: str, redis_url: str):
        self.rabbitmq_url = rabbitmq_url
        self.redis_url = redis_url
        self.connection = None
        self.channel = None
        self.redis_client = None

    async def connect(self):
        """connect to rabbitmq and redis"""
        self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)

        self.redis_client = redis.from_url(self.redis_url)

        logger.info(f"Connected to RabbitMQ: {self.rabbitmq_url}")
        logger.info(f"Connected to Redis: {self.redis_url}")

    async def close(self):
        if self.connection:
            await self.connection.close()
        if self.redis_client:
            await self.redis_client.close()

        logger.info("Worker closed")

    async def update_task_status(self, task_id: str, status: str, result=None, error=None):
        """update task status"""
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

            # write back to Redis
            await self.redis_client.set(task_key, json.dumps(task), ex=3600)
            logger.info(f"Task {task_id} status updated to: {status}")
            return task
        
        except Exception as e:
            logger.error(f"Failed to update task status for {task_id}: {e}")
            raise

    async def process_task(self, task_data: dict):
        try:
            task_id = task_data.get("task_id")
            task_type = task_data.get("task_type")
            parameters = task_data.get("parameters")

            print(f"⚙️  [Worker] Processing task {task_id} with type: {task_type} ...")
            logger.info(f"⚙️  [Worker] Processing task {task_id} with type: {task_type} ...")

            handler = TaskHandlerFactory.get_handler(task_type)
            result = await handler.handle(parameters)

            # update task status
            await self.update_task_status(task_id, "completed", result=result, error=None)

            print(f"🎯 [Worker] Task {task_id} completed with result: {result}")
            logger.info(f"🎯 [Worker] Task {task_id} completed with result: {result}")

        except Exception as e:
            print(f"💥 [Worker] Error processing task {task_id}: {e}")
            logger.error(f"💥 [Worker] Error processing task {task_id}: {e}")
            raise
    
    async def handle_message(self, message):
        """handle incoming message from rabbitmq"""
        try:
            async with message.process():
                body = message.body.decode('utf-8')
                task_data = json.loads(body)
                
                task_id = task_data.get("task_id")
                task_type = task_data.get("task_type")
                
                # 添加消费消息的输出
                print(f"🔄 [Worker] Consuming task: {task_id} (type: {task_type})")
                logger.info(f"🔄 [Worker] Consuming task: {task_id} (type: {task_type})")
                
                # update task status to running
                await self.update_task_status(task_id, "running")

                # process task  
                await self.process_task(task_data)

                print(f"✅ [Worker] Task {task_id} processed successfully")
                logger.info(f"✅ [Worker] Task {task_id} processed successfully")

        except Exception as e:
            print(f"❌ [Worker] Error processing message: {e}")
            logger.error(f"❌ [Worker] Error processing message: {e}")
            raise

    async def run(self):
        """run the worker"""
        try:
            await self.connect()

            queue = await self.channel.declare_queue("task_queue", durable=True)
            await queue.consume(self.handle_message)

            logger.info("Worker started, waiting for tasks...")

            # run forever
            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in worker: {e}")
            raise
    
async def main():
    """main function"""
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/")
    worker = TaskWorker(rabbitmq_url, redis_url)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())