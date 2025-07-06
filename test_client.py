#!/usr/bin/env python3
"""
Test class for TaskSubmitter - Test Redis and RabbitMQ storage
"""
import unittest
import requests
import json
import time
import pika
from client.task_generator import TaskSubmitter

class TestClient(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.api_url = "http://localhost:8000"
        self.submitter = TaskSubmitter(api_url=self.api_url, interval=0.1)
    
    def test_redis(self):
        """Test that submitted task is stored in both Redis and RabbitMQ"""

        task_data = {
            "task_type": "calculator",
            "parameters": {"a": 10, "b": 5, "operation": "add"},
            "priority": 1
        }
        
        success = self.submitter.submit_task(task_data)
        self.assertTrue(success, "Task submission should succeed")
        
        # Wait a moment for processing
        time.sleep(1)
        
        # Check if task exists in Redis via API
        try:
            response = requests.get(f"{self.api_url}/tasks")
            self.assertEqual(response.status_code, 200, "Should be able to get tasks")
            
            tasks = response.json()
            self.assertGreater(len(tasks), 0, "Should have at least one task")
            
            # Get the latest task (assuming it's the one we just submitted)
            latest_task = tasks[-1]
            task_id = latest_task["task_id"]
            
            task_response = requests.get(f"{self.api_url}/tasks/{task_id}")
            self.assertEqual(task_response.status_code, 200, f"Task {task_id} should exist in Redis")
            
            retrieved_task = task_response.json()
            self.assertEqual(retrieved_task["task_type"], "calculator")
            self.assertEqual(retrieved_task["parameters"]["a"], 10)
            self.assertEqual(retrieved_task["parameters"]["b"], 5)
            self.assertEqual(retrieved_task["parameters"]["operation"], "add")
            
            print(f"✅ Task {task_id} successfully stored in Redis")
            
        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to check Redis storage: {e}")
    

    def test_rabbitmq_queue_has_messages(self):
        """Test that RabbitMQ queue contains messages"""
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost', 5672, '/', 
                                        pika.PlainCredentials('admin', 'admin123'))
            )
            channel = connection.channel()
            
            queue_name = 'task_queue'
            channel.queue_declare(queue=queue_name, durable=True)
            
            # get queue status
            queue_info = channel.queue_declare(queue=queue_name, durable=True, passive=True)
            message_count = queue_info.method.message_count
            
            print(f"📊 Queue has {message_count} messages")

            self.assertGreater(message_count, 0, f"Queue should have messages, but found {message_count}")
            
            # get and validate message
            method_frame, header_frame, body = channel.basic_get(queue_name, auto_ack=False)
            
            self.assertIsNotNone(method_frame, "Should be able to get message from queue")
            self.assertIsNotNone(body, "Message body should not be None")
            
            # validate message format
            message_data = json.loads(body.decode('utf-8'))
            self.assertIn('task_type', message_data)
            self.assertIn('parameters', message_data)
            self.assertIn('task_id', message_data)
            
            print(f"✅ Message validated: {message_data['task_type']} task")
            
            # ack message
            channel.basic_ack(method_frame.delivery_tag)
            connection.close()
            
        except Exception as e:
            self.fail(f"RabbitMQ test failed: {e}")

   

def main():
    """Run all tests"""
    print("🚀 Starting Task Storage Tests...")
    print("=" * 50)
    
    # Run unit tests
    unittest.main(verbosity=2, exit=False)
    
    print("\n" + "=" * 50)
    print("✅ All tests completed!")

if __name__ == "__main__":
    main()