#!/usr/bin/env python3
"""
Test script for refactored worker architecture
"""

import asyncio
import logging
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from worker.connection_manager import ConnectionManager
from worker.worker_factory import WorkerFactory
from worker.worker_registry import WorkerRegistry
from worker.lifecycle_manager import WorkerLifecycleManager
from worker.distributed_worker import DistributedWorker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_connection_manager():
    """Test ConnectionManager"""
    logger.info("Testing ConnectionManager...")
    
    connection_manager = ConnectionManager(
        rabbitmq_url="amqp://admin:admin123@localhost:5672/",
        redis_url="redis://localhost:6379/0"
    )
    
    try:
        connection, channel, redis_client = await connection_manager.create_connections()
        logger.info("✅ ConnectionManager test passed")
        return connection, channel, redis_client, connection_manager
    except Exception as e:
        logger.error(f"❌ ConnectionManager test failed: {e}")
        raise

async def test_worker_registry(redis_client):
    """Test WorkerRegistry"""
    logger.info("Testing WorkerRegistry...")
    
    registry = WorkerRegistry(redis_client)
    
    # Test registration
    worker_info = {
        "worker_id": "test-worker-1",
        "status": "running",
        "last_heartbeat": "2024-01-01T00:00:00",
        "start_time": "2024-01-01T00:00:00",
        "processed_tasks": 0,
        "process_id": 12345,
    }
    
    try:
        await registry.register_worker("test-worker-1", worker_info)
        logger.info("✅ Worker registration test passed")
        
        # Test heartbeat update
        await registry.update_heartbeat("test-worker-1")
        logger.info("✅ Heartbeat update test passed")
        
        # Test unregistration
        await registry.unregister_worker("test-worker-1")
        logger.info("✅ Worker unregistration test passed")
        
    except Exception as e:
        logger.error(f"❌ WorkerRegistry test failed: {e}")
        raise

async def test_worker_factory(connection, channel, redis_client):
    """Test WorkerFactory"""
    logger.info("Testing WorkerFactory...")
    
    try:
        worker = await WorkerFactory.create_worker(connection, channel, redis_client)
        logger.info("✅ WorkerFactory test passed")
        return worker
    except Exception as e:
        logger.error(f"❌ WorkerFactory test failed: {e}")
        raise

async def test_lifecycle_manager_with_timeout(worker, registry):
    """Test WorkerLifecycleManager with timeout to avoid infinite loop"""
    logger.info("Testing WorkerLifecycleManager with timeout...")
    
    lifecycle_manager = WorkerLifecycleManager(worker, registry, "test-worker-3")
    
    try:
        # Start lifecycle in background
        start_task = asyncio.create_task(lifecycle_manager.start())
        
        # Wait for a short time to let it start
        await asyncio.sleep(2)
        
        # Stop lifecycle
        await lifecycle_manager.stop()
        
        # Cancel the start task if it's still running
        if not start_task.done():
            start_task.cancel()
            try:
                await start_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ LifecycleManager timeout test passed")
        
    except Exception as e:
        logger.error(f"❌ LifecycleManager timeout test failed: {e}")
        raise

async def test_distributed_worker_with_timeout():
    """Test DistributedWorker with timeout"""
    logger.info("Testing DistributedWorker with timeout...")
    
    worker = DistributedWorker(
        worker_id="test-distributed-worker",
        rabbitmq_url="amqp://admin:admin123@localhost:5672/",
        redis_url="redis://localhost:6379/0"
    )
    
    try:
        # Start the worker in background
        start_task = asyncio.create_task(worker.start())
        
        # Wait for a short time to let it start
        await asyncio.sleep(3)
        
        # Stop the worker
        await worker.stop()
        
        # Cancel the start task if it's still running
        if not start_task.done():
            start_task.cancel()
            try:
                await start_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ DistributedWorker timeout test passed")
        
    except Exception as e:
        logger.error(f"❌ DistributedWorker timeout test failed: {e}")
        raise

async def main():
    """Run all tests"""
    logger.info("Starting refactored worker architecture tests...")
    
    connection_manager = None
    
    try:
        # Test individual components
        connection, channel, redis_client, connection_manager = await test_connection_manager()
        
        await test_worker_registry(redis_client)
        
        worker = await test_worker_factory(connection, channel, redis_client)
        
        registry = WorkerRegistry(redis_client)
        
        # Test lifecycle manager with timeout
        await test_lifecycle_manager_with_timeout(worker, registry)
        
        # Test the complete flow with timeout
        await test_distributed_worker_with_timeout()
        
        logger.info("🎉 All tests passed!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
    finally:
        # Cleanup
        if connection_manager:
            await connection_manager.close_connections()

if __name__ == "__main__":
    asyncio.run(main()) 