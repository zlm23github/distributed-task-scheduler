#!/usr/bin/env python3
"""
Task Submitter - Continuously submits mock tasks to the API
"""
import requests
import json
import time
import random
from typing import Dict, Any

class TaskSubmitter:
    def __init__(self, api_url: str = "http://localhost:8000", interval: int = 1):
        """
        Initialize task submitter
        
        Args:
            api_url: API server URL
            interval: Seconds between task submissions
        """
        self.api_url = api_url
        self.interval = interval
        self.session = requests.Session()
        self.task_count = 0
        
    def generate_mock_task(self) -> Dict[str, Any]:
        """Generate a random mock task based on models.py"""
        # Only use task types defined in models.py
        task_types = ["web_scraper", "calculator"]
        
        task_type = random.choice(task_types)
        
        if task_type == "web_scraper":
            parameters = {
                "url": f"https://httpbin.org/{random.choice(['json', 'html', 'xml'])}",
                "selector": random.choice(["h1", "h2", "p", "title", "body"]),
                "timeout": random.randint(5, 30)
            }
        else:  # calculator
            operations = ["add", "subtract", "multiply", "divide", "power"]
            parameters = {
                "a": random.randint(1, 100),
                "b": random.randint(1, 100),
                "operation": random.choice(operations)
            }
        
        return {
            "task_type": task_type,
            "parameters": parameters,
            "priority": random.randint(1, 5)
        }
    
    def submit_task(self, task_data: Dict[str, Any]) -> bool:
        """Submit a task to the API"""
        try:
            response = self.session.post(
                f"{self.api_url}/tasks",
                json=task_data,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                task = response.json()
                self.task_count += 1
                print(f"✅ Task #{self.task_count} submitted: {task['task_id']} ({task['task_type']})")
                return True
            else:
                print(f"❌ Failed to submit task: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            return False
    
    def run(self, max_tasks: int = None):
        """
        Run the task submitter
        
        Args:
            max_tasks: Stop after submitting this many tasks (None = run forever)
        """
        print(f"🚀 Starting Task Submitter...")
        print(f"   API URL: {self.api_url}")
        print(f"   Interval: {self.interval} seconds")
        print(f"   Max tasks: {max_tasks or 'unlimited'}")
        print(f"   Task types: web_scraper, calculator")
        print("-" * 50)
        
        try:
            while True:
                # Check if we should stop
                if max_tasks and self.task_count >= max_tasks:
                    print(f"\n📊 Task limit reached ({max_tasks} tasks)")
                    break
                
                # Generate and submit task
                task_data = self.generate_mock_task()
                self.submit_task(task_data)
                
                # Wait for next interval
                time.sleep(self.interval)
                
        except KeyboardInterrupt:
            print(f"\n🛑 Task Submitter stopped by user")
        
        finally:
            print(f"\n📊 Summary: Submitted {self.task_count} tasks")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Task Submitter - Submit mock tasks continuously")
    parser.add_argument("--api-url", default="http://localhost:8000",
                       help="API server URL (default: http://localhost:8000)")
    parser.add_argument("--interval", "-i", type=int, default=1,
                       help="Seconds between task submissions (default: 1)")
    parser.add_argument("--max-tasks", "-m", type=int,
                       help="Stop after submitting this many tasks (default: run forever)")
    
    args = parser.parse_args()
    
    submitter = TaskSubmitter(api_url=args.api_url, interval=args.interval)
    submitter.run(max_tasks=args.max_tasks)

if __name__ == "__main__":
    main() 