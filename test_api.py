import requests
import json
import time
from typing import Dict, Any

class TaskAPITester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def test_health_check(self):
        """Test health check endpoint"""
        print("Testing health check...")
        response = self.session.get(f"{self.base_url}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        print("Health check passed")
    
    def test_create_task(self) -> str:
        """Test task creation (API only creates, doesn't execute)"""
        print("Testing task creation...")
        
        # Create web scraping task
        task_data = {
            "task_type": "web_scraper",
            "parameters": {
                "url": "https://example.com",
                "selector": "h1"
            },
            "priority": 2
        }
        
        response = self.session.post(
            f"{self.base_url}/tasks",
            json=task_data,
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200
        task = response.json()
        assert task["task_type"] == "web_scraper"
        assert task["status"] == "pending"  
        assert task["result"] is None       
        assert "task_id" in task
        
        print(f"Task created with ID: {task['task_id']}")
        print(f"   Status: {task['status']} (waiting for worker to process)")
        return task["task_id"]
    
    def test_get_task(self, task_id: str):
        """Test get single task"""
        print(f"Testing get task {task_id}...")
        
        response = self.session.get(f"{self.base_url}/tasks/{task_id}")
        assert response.status_code == 200
        
        task = response.json()
        assert task["task_id"] == task_id
        print(f"Task status: {task['status']}")
        return task
    
    def test_get_all_tasks(self):
        """Test get all tasks"""
        print("Testing get all tasks...")
        
        response = self.session.get(f"{self.base_url}/tasks")
        assert response.status_code == 200
        
        tasks = response.json()
        assert isinstance(tasks, list)
        
        # Show task status distribution
        status_counts = {}
        for task in tasks:
            status = task["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"Found {len(tasks)} tasks")
        print(f"   Status distribution: {status_counts}")
    
    def test_update_task(self, task_id: str):
        """Test manual task status update (simulating Worker update)"""
        print(f"Testing manual update task {task_id}...")
        
        # Simulate Worker starting to process task
        update_data = {"status": "running"}
        response = self.session.put(
            f"{self.base_url}/tasks/{task_id}",
            json=update_data,
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200
        task = response.json()
        assert task["status"] == "running"
        print("Task updated to running (simulating worker processing)")
        
        # Simulate Worker completing task
        update_data = {
            "status": "completed",
            "result": {
                "scraped_data": "Hello World from example.com",
                "url": "https://example.com",
                "processed_at": "2024-01-01T10:05:00Z"
            }
        }
        response = self.session.put(
            f"{self.base_url}/tasks/{task_id}",
            json=update_data,
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200
        task = response.json()
        assert task["status"] == "completed"
        assert task["result"] is not None
        print("Task updated to completed (simulating worker finished)")
        print(f"   Result: {task['result']}")
    
    
    def test_delete_task(self, task_id: str):
        """Test delete task"""
        print(f"Testing delete task {task_id}...")
        
        response = self.session.delete(f"{self.base_url}/tasks/{task_id}")
        assert response.status_code == 200
        
        result = response.json()
        assert result == True  # 现在返回的是布尔值
        print("Task deleted")
    
    
    
    def run_all_tests(self):
        """Run all tests"""
        print("Starting API tests...")
        print("Note: This API manages tasks, workers execute them")
        
        try:
            self.test_health_check()

            # Create task and save the task_id
            task_id = self.test_create_task()
            
            self.test_get_all_tasks()
            
            self.test_get_task(task_id)

            self.test_update_task(task_id)
            
            self.test_delete_task(task_id)
            
            print("All tests passed!")
            
        except Exception as e:
            print(f"Test failed: {e}")
            raise

if __name__ == "__main__":
    tester = TaskAPITester()
    tester.run_all_tests() 