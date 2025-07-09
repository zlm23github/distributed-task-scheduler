import logging
import json
import redis.asyncio as redis
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class WorkerRegistry:
    """Manages worker registration and heartbeat"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
    
    async def register_worker(self, worker_id: str, worker_info: Dict[str, Any]):
        """
        Register a worker in Redis
        
        Args:
            worker_id: Unique worker identifier
            worker_info: Worker information dictionary
        """
        try:
            key = f"worker:{worker_id}"
            await self.redis_client.set(key, json.dumps(worker_info), ex=300)
            logger.info(f"Worker {worker_id} registered successfully")
        except Exception as e:
            logger.error(f"Failed to register worker {worker_id}: {e}")
            raise
    
    async def update_heartbeat(self, worker_id: str):
        """
        Update worker heartbeat
        
        Args:
            worker_id: Worker identifier
        """
        try:
            worker_data = await self.redis_client.get(f"worker:{worker_id}")
            if worker_data:
                worker_info = json.loads(worker_data)
                # Update last_heartbeat
                worker_info["last_heartbeat"] = datetime.utcnow().isoformat()

                # Write back to Redis
                await self.redis_client.set(f"worker:{worker_id}", json.dumps(worker_info), ex=300)
            else:
                logger.warning(f"Worker {worker_id} not found in Redis")

        except Exception as e:
            logger.error(f"Failed to update heartbeat for worker {worker_id}: {e}")
    
    async def unregister_worker(self, worker_id: str):
        """
        Unregister a worker from Redis
        
        Args:
            worker_id: Worker identifier
        """
        try:
            await self.redis_client.delete(f"worker:{worker_id}")
            logger.info(f"Worker {worker_id} unregistered successfully")
        except Exception as e:
            logger.error(f"Failed to unregister worker {worker_id}: {e}")
    
    async def get_worker_info(self, worker_id: str) -> Dict[str, Any]:
        """
        Get worker information from Redis
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            Worker information dictionary or None if not found
        """
        try:
            worker_data = await self.redis_client.get(f"worker:{worker_id}")
            if worker_data:
                return json.loads(worker_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get worker info for {worker_id}: {e}")
            return None
    
    async def get_all_workers(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all registered workers
        
        Returns:
            Dictionary of worker_id -> worker_info
        """
        try:
            workers = {}
            async for key in self.redis_client.scan_iter(match="worker:*"):
                worker_id = key.decode('utf-8').replace("worker:", "")
                worker_info = await self.get_worker_info(worker_id)
                if worker_info:
                    workers[worker_id] = worker_info
            return workers
        except Exception as e:
            logger.error(f"Failed to get all workers: {e}")
            return {} 