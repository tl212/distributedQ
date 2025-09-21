#!/usr/bin/env python
"""
Test the Distributed Queue API
"""

import requests
import json
import time
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

def test_api():
    print("Testing Distributed Queue API")
    print("=" * 50)
    
    # Test root endpoint
    print("\n1. Testing root endpoint...")
    response = requests.get(f"{BASE_URL}/")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Test health check
    print("\n2. Testing health check...")
    response = requests.get(f"{BASE_URL}/api/v1/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Test queue stats
    print("\n3. Getting queue stats...")
    response = requests.get(f"{BASE_URL}/api/v1/stats")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Submit a simple task
    print("\n4. Submitting a simple task...")
    task_data = {
        "priority": 5,
        "payload": {
            "type": "simple",
            "data": "Test task from API"
        },
        "max_retries": 3
    }
    response = requests.post(f"{BASE_URL}/api/v1/tasks", json=task_data)
    print(f"Status: {response.status_code}")
    task_response = response.json()
    print(f"Response: {json.dumps(task_response, indent=2)}")
    task_id = task_response.get("task_id")
    
    # Submit a math task
    print("\n5. Submitting a math task...")
    math_task = {
        "priority": 1,
        "payload": {
            "type": "math",
            "data": {
                "operation": "add",
                "a": 10,
                "b": 20
            }
        }
    }
    response = requests.post(f"{BASE_URL}/api/v1/tasks", json=math_task)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Submit a delayed task
    print("\n6. Submitting a delayed task (5 seconds)...")
    delayed_task = {
        "priority": 0,
        "payload": {
            "type": "simple",
            "data": "Delayed task"
        },
        "execute_after": (datetime.now() + timedelta(seconds=5)).isoformat()
    }
    response = requests.post(f"{BASE_URL}/api/v1/tasks", json=delayed_task)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Wait a bit for processing
    print("\n7. Waiting for task processing...")
    time.sleep(2)
    
    # Get task status
    if task_id:
        print(f"\n8. Getting task status for {task_id}...")
        response = requests.get(f"{BASE_URL}/api/v1/tasks/{task_id}")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Get updated stats
    print("\n9. Getting updated queue stats...")
    response = requests.get(f"{BASE_URL}/api/v1/stats")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Get system info
    print("\n10. Getting system info...")
    response = requests.get(f"{BASE_URL}/info")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    print("\n" + "=" * 50)
    print("API test completed successfully!")

if __name__ == "__main__":
    try:
        test_api()
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to API server at http://localhost:8000")
        print("Please make sure the server is running: python run_api.py")