import logging
import asyncio
import os
from datetime import datetime
from typing import Dict, Any

from worker.task_worker import TaskWorker
from worker.worker_registry import WorkerRegistry

logger = logging.getLogger(__name__)

class WorkerLifecycleManager:
    """Manages worker lifecycle including heartbeat and start/stop operations"""
    
    def __init__(self, worker: TaskWorker, registry: WorkerRegistry, worker_id: str):
        self.worker = worker
        self.registry = registry
        self.worker_id = worker_id
        self.running = False
        self.start_time = datetime.utcnow()
        self.heartbeat_task = None
    
    async def start(self):
        """Start the worker lifecycle"""
        try:
            logger.info(f"Starting worker lifecycle for {self.worker_id}")
            
            # Register worker
            await self._register_worker()
            
            # Set running flag
            self.running = True
            
            # Start heartbeat loop in background
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            # Start task processing
            await self.worker.process_tasks()
            
        except Exception as e:
            logger.error(f"Failed to start worker lifecycle for {self.worker_id}: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the worker lifecycle"""
        try:
            logger.info(f"Stopping worker lifecycle for {self.worker_id}")
            self.running = False
            
            # Cancel heartbeat task
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            # Close worker connections
            await self.worker.close()
            
            # Unregister worker
            await self._unregister_worker()
            
            logger.info(f"Worker lifecycle stopped for {self.worker_id}")
            
        except Exception as e:
            logger.error(f"Error stopping worker lifecycle for {self.worker_id}: {e}")
    
    async def _register_worker(self):
        """Register worker with registry"""
        worker_info = {
            "worker_id": self.worker_id,
            "status": "running",
            "last_heartbeat": datetime.utcnow().isoformat(),
            "start_time": self.start_time.isoformat(),
            "processed_tasks": 0,
            "process_id": os.getpid(),
        }
        
        await self.registry.register_worker(self.worker_id, worker_info)
    
    async def _unregister_worker(self):
        """Unregister worker from registry"""
        await self.registry.unregister_worker(self.worker_id)
    
    async def _heartbeat_loop(self):
        """Heartbeat loop for worker"""
        while self.running:
            try:
                await self.registry.update_heartbeat(self.worker_id)
                await asyncio.sleep(30)  # Update heartbeat every 30 seconds
            except Exception as e:
                logger.error(f"Heartbeat loop error for {self.worker_id}: {e}")
                await asyncio.sleep(5)  # Sleep 5 seconds if error
    
    def is_running(self) -> bool:
        """Check if worker is running"""
        return self.running 