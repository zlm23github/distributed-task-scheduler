#!/usr/bin/env python3
"""
Distributed Task Worker Manager
Supports multiple workers with load balancing and monitoring
"""

import asyncio
import logging
import uuid
import signal
import sys
import os
from typing import Optional

from worker.connection_manager import ConnectionManager
from worker.worker_factory import WorkerFactory
from worker.worker_registry import WorkerRegistry
from worker.lifecycle_manager import WorkerLifecycleManager

logger = logging.getLogger(__name__)

class DistributedWorker:
    """Main coordinator for distributed worker operations"""
    
    def __init__(self, worker_id: Optional[str] = None, rabbitmq_url: str = None, redis_url: str = None):
        self.worker_id = worker_id or str(uuid.uuid4())
        self.rabbitmq_url = rabbitmq_url or "amqp://admin:admin123@localhost:5672/"
        self.redis_url = redis_url or "redis://localhost:6379/0"
        
        # Initialize components
        self.connection_manager = ConnectionManager(self.rabbitmq_url, self.redis_url)
        self.registry = None
        self.lifecycle_manager = None
    
    async def start(self):
        """Start the distributed worker"""
        try:
            logger.info(f"Starting distributed worker {self.worker_id}")
            
            # 1. Create connections
            connection, channel, redis_client = await self.connection_manager.create_connections()
            
            # 2. Create registry
            self.registry = WorkerRegistry(redis_client)
            
            # 3. Create worker using factory
            worker = await WorkerFactory.create_worker(connection, channel, redis_client)
            
            # 4. Create lifecycle manager
            self.lifecycle_manager = WorkerLifecycleManager(worker, self.registry, self.worker_id)
            
            # 5. Start lifecycle
            await self.lifecycle_manager.start()
            
        except Exception as e:
            logger.error(f"Failed to start distributed worker {self.worker_id}: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the distributed worker"""
        try:
            logger.info(f"Stopping distributed worker {self.worker_id}")
            
            # Stop lifecycle manager
            if self.lifecycle_manager:
                await self.lifecycle_manager.stop()
            
            # Close connections
            await self.connection_manager.close_connections()
            
            logger.info(f"Distributed worker {self.worker_id} stopped")
            
        except Exception as e:
            logger.error(f"Error stopping distributed worker {self.worker_id}: {e}")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down worker {self.worker_id}")
        asyncio.create_task(self.stop())
    
    def run(self):
        """Run the worker with signal handling"""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        try:
            # Run the async event loop
            asyncio.run(self.start())
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
        except Exception as e:
            logger.error(f"Worker run error: {e}")
            sys.exit(1)


def main():
    """Command line interface for distributed worker"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Distributed Task Worker")
    parser.add_argument("--worker-id", help="Worker ID (auto-generated if not provided)")
    parser.add_argument("--rabbitmq-url", default="amqp://admin:admin123@localhost:5672/", 
                       help="RabbitMQ connection URL")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", 
                       help="Redis connection URL")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run worker
    worker = DistributedWorker(
        worker_id=args.worker_id,
        rabbitmq_url=args.rabbitmq_url,
        redis_url=args.redis_url
    )
    
    worker.run()


if __name__ == "__main__":
    main()
    
    
    