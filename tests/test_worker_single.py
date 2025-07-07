#!/usr/bin/env python3
"""
Single Worker test class
Tests Worker connection, task processing, status updates, etc.

REQUIRED SERVICES:
Before running this test, ensure the following services are running:

1. API Server (port 8000):
   cd api_server && python -m uvicorn app.main:app --host 0.0.0.0 --port 8000

2. RabbitMQ (port 5672):
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   # Or use docker-compose: docker-compose up -d rabbitmq

3. Redis (port 6379):
   docker run -d --name redis -p 6379:6379 redis:7-alpine
   # Or use docker-compose: docker-compose up -d redis

4. Worker :
   python -m worker.worker

5. Or start all services with docker-compose:
   docker-compose up -d
"""
import unittest
import asyncio
import json
import time
import requests
from worker.task_worker import TaskWorker
from worker.task_handlers import TaskHandlerFactory, CalculatorTaskHandler, WebScraperTaskHandler

class TestSingleWorker(unittest.TestCase):
    
    def setUp(self):
        self.api_url = "http://localhost:8000"
        self.rabbitmq_url = "amqp://admin:admin123@localhost:5672/"
        self.redis_url = "redis://localhost:6379/"
        self.worker = None
    
    def tearDown(self):
        if self.worker:
            asyncio.run(self.worker.close())
    
    def test_worker_connection(self):
        async def test_connect():
            self.worker = TaskWorker(self.rabbitmq_url, self.redis_url)
            await self.worker.connect()
            
            self.assertIsNotNone(self.worker.connection)
            self.assertIsNotNone(self.worker.channel)
            self.assertIsNotNone(self.worker.redis_client)
            
            await self.worker.close()
        
        asyncio.run(test_connect())
        print("✅ Worker connection test passed")
    
    def test_task_handler_factory(self):
        calc_handler = TaskHandlerFactory.get_handler("calculator")
        self.assertIsInstance(calc_handler, CalculatorTaskHandler)
        
        scraper_handler = TaskHandlerFactory.get_handler("web_scraper")
        self.assertIsInstance(scraper_handler, WebScraperTaskHandler)
        
        with self.assertRaises(ValueError):
            TaskHandlerFactory.get_handler("unknown_task")
        
        print("✅ Task handler factory test passed")
    
    def test_calculator_handler(self):
        async def test_calc():
            handler = CalculatorTaskHandler()
            
            result = await handler.handle({"a": 10, "b": 5, "operation": "add"})
            self.assertIn("result", result)
            self.assertIn("Calculator processed: 10 add 5", result["result"])
            
            result = await handler.handle({"a": 3, "b": 4, "operation": "multiply"})
            self.assertIn("Calculator processed: 3 multiply 4", result["result"])
            
            result = await handler.handle({"a": 7, "b": 3})
            self.assertIn("Calculator processed: 7 add 3", result["result"])
        
        asyncio.run(test_calc())
        print("✅ Calculator handler test passed")
    
    def test_web_scraper_handler(self):
        async def test_scraper():
            handler = WebScraperTaskHandler()
            
            result = await handler.handle({"url": "https://httpbin.org/html"})
            self.assertIn("result", result)
            self.assertIn("Web scraper processed URL: https://httpbin.org/html", result["result"])
        
        asyncio.run(test_scraper())
        print("✅ Web scraper handler test passed")
    
    # def test_worker_task_processing(self):
    #     async def test_processing():
    #         self.worker = TaskWorker(self.rabbitmq_url, self.redis_url)
    #         await self.worker.connect()
            
    #         task_data = {
    #             "task_id": "test-task-123",
    #             "task_type": "calculator",
    #             "parameters": {"a": 15, "b": 3, "operation": "add"},
    #             "priority": 1
    #         }
            
    #         await self.worker.process_task(task_data)
            
    #         task_key = f"task:{task_data['task_id']}"
    #         task_data_redis = await self.worker.redis_client.get(task_key)
            
    #         if task_data_redis:
    #             task = json.loads(task_data_redis)
    #             self.assertEqual(task["status"], "completed")
    #             self.assertIn("result", task)
    #         else:
    #             self.fail("Task data not written to Redis")
            
    #         await self.worker.close()
        
    #     asyncio.run(test_processing())
    #     print("✅ Worker task processing test passed")
    
    def test_worker_status_update(self):
        async def test_status_update():
            self.worker = TaskWorker(self.rabbitmq_url, self.redis_url)
            await self.worker.connect()
            
            test_task = {
                "task_id": "test-status-456",
                "status": "pending",
                "task_type": "calculator",
                "parameters": {"a": 1, "b": 2},
                "priority": 1,
                "created_at": "2024-01-01T00:00:00Z"
            }
            
            task_key = f"task:{test_task['task_id']}"
            await self.worker.redis_client.set(task_key, json.dumps(test_task), ex=3600)
            
            await self.worker.update_task_status(test_task["task_id"], "running")
            
            updated_task_data = await self.worker.redis_client.get(task_key)
            updated_task = json.loads(updated_task_data)
            self.assertEqual(updated_task["status"], "running")
            self.assertIn("started_at", updated_task)
            
            await self.worker.update_task_status(test_task["task_id"], "completed", result={"test": "result"})
            
            completed_task_data = await self.worker.redis_client.get(task_key)
            completed_task = json.loads(completed_task_data)
            self.assertEqual(completed_task["status"], "completed")
            self.assertIn("completed_at", completed_task)
            self.assertIn("result", completed_task)
            
            await self.worker.close()
        
        asyncio.run(test_status_update())
        print("✅ Worker status update test passed")
    
    def test_end_to_end_workflow(self):
        # Integration test: submit task via API, wait for processing, check result
        task_data = {
            "task_type": "calculator",
            "parameters": {"a": 20, "b": 10, "operation": "subtract"},
            "priority": 1
        }
        
        try:
            # submit task via API
            response = requests.post(f"{self.api_url}/tasks", json=task_data)
            self.assertEqual(response.status_code, 200)
            
            task_response = response.json()
            task_id = task_response["task_id"]
            
            print(f"✅ Task submitted successfully, ID: {task_id}")
            
            time.sleep(2)

            # check task status
            max_wait = 10 
            for i in range(max_wait):
                time.sleep(1)
                status_response = requests.get(f"{self.api_url}/tasks/{task_id}")
                task_status = status_response.json()
                
                if task_status["status"] == "completed":
                    self.assertIn("result", task_status)
                    print(f"✅ Task completed, result: {task_status['result']}")
                    return 
                elif task_status["status"] == "failed":
                    self.fail(f"Task failed: {task_status.get('error', 'Unknown error')}")
            
            # if timeout, fail the test
            self.fail(f"Task did not complete within {max_wait} seconds, status: {task_status['status']}")
            
        except requests.exceptions.RequestException as e:
            self.fail(f"API request failed: {e}")
        
        print("✅ End-to-end workflow test passed")

def run_worker_tests():
    print("🚀 Starting Worker functionality tests...")
    print("=" * 60)
    
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestSingleWorker)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    print("=" * 60)
    if result.wasSuccessful():
        print("✅ All Worker tests passed!")
    else:
        print("❌ Some tests failed, please check error messages")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_worker_tests()
    
    if success:
        print("\n🎉 Worker tests completed!")
    else:
        print("\n⚠️  Please fix test failures before starting Worker") 