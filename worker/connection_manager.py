import logging
import aio_pika
import redis.asyncio as redis

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages RabbitMQ and Redis connections"""
    
    def __init__(self, rabbitmq_url: str, redis_url: str):
        self.rabbitmq_url = rabbitmq_url
        self.redis_url = redis_url
        self.connection = None
        self.channel = None
        self.redis_client = None
    
    async def create_connections(self):
        """Establish RabbitMQ and Redis connections"""
        try:
            # Create RabbitMQ connection
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()
            
            # Create Redis connection
            self.redis_client = redis.from_url(self.redis_url)
            
            logger.info(f"Connected to RabbitMQ: {self.rabbitmq_url}")
            logger.info(f"Connected to Redis: {self.redis_url}")
            
            return self.connection, self.channel, self.redis_client
            
        except Exception as e:
            logger.error(f"Failed to create connections: {e}")
            await self.close_connections()
            raise
    
    async def close_connections(self):
        """Close all connections"""
        try:
            if self.connection:
                await self.connection.close()
                logger.info("RabbitMQ connection closed")
            
            if self.redis_client:
                await self.redis_client.close()
                logger.info("Redis connection closed")
                
        except Exception as e:
            logger.error(f"Error closing connections: {e}")
    
    def get_connection(self):
        """Get RabbitMQ connection"""
        return self.connection
    
    def get_channel(self):
        """Get RabbitMQ channel"""
        return self.channel
    
    def get_redis_client(self):
        """Get Redis client"""
        return self.redis_client 