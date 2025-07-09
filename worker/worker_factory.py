import logging
from worker.task_worker import TaskWorker

logger = logging.getLogger(__name__)

class WorkerFactory:
    """Factory for creating TaskWorker instances"""
    
    @staticmethod
    async def create_worker(connection, channel, redis_client) -> TaskWorker:
        """
        Create a TaskWorker instance with established connections
        
        Args:
            connection: Established RabbitMQ connection
            channel: Established RabbitMQ channel
            redis_client: Established Redis client
            
        Returns:
            TaskWorker instance
        """
        try:
            worker = TaskWorker(connection, channel, redis_client)
            logger.info("TaskWorker created successfully")
            return worker
        except Exception as e:
            logger.error(f"Failed to create TaskWorker: {e}")
            raise 