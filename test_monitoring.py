#!/usr/bin/env python
"""
Test script for monitoring features
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"

def test_monitoring():
    print("Testing Monitoring Features")
    print("=" * 50)
    
    # Test health endpoint
    print("\n1. Testing Health Endpoint...")
    response = requests.get(f"{BASE_URL}/api/v1/health")
    if response.status_code == 200:
        health = response.json()
        print(f"✅ Health Status: {health['status']}")
        print(f"   Uptime: {health['uptime_seconds']:.1f} seconds")
        print(f"   Checks: {json.dumps(health['checks'], indent=2)}")
    else:
        print(f"❌ Health check failed: {response.status_code}")
    
    # Test metrics endpoint
    print("\n2. Testing Metrics Endpoint...")
    response = requests.get(f"{BASE_URL}/api/v1/metrics")
    if response.status_code == 200:
        metrics = response.json()
        print("✅ Metrics Retrieved:")
        print(f"   Tasks Submitted: {metrics['counters']['tasks_submitted']}")
        print(f"   Tasks Completed: {metrics['counters']['tasks_completed']}")
        print(f"   Queue Depth: {metrics['gauges']['queue_depth']}")
        print(f"   Success Rate: {metrics['rates']['success_rate']:.1f}%")
    else:
        print(f"❌ Metrics fetch failed: {response.status_code}")
    
    # Test Prometheus endpoint
    print("\n3. Testing Prometheus Endpoint...")
    response = requests.get(f"{BASE_URL}/api/v1/metrics/prometheus")
    if response.status_code == 200:
        prometheus_data = response.text
        lines = prometheus_data.split('\n')[:5]  # Show first 5 lines
        print("✅ Prometheus Format:")
        for line in lines:
            print(f"   {line}")
        print("   ...")
    else:
        print(f"❌ Prometheus metrics failed: {response.status_code}")
    
    # Submit some tasks to generate metrics
    print("\n4. Generating Test Load...")
    print("   Submitting tasks to generate metrics...")
    
    # Submit various types of tasks
    task_types = [
        {"type": "simple", "data": "Test task"},
        {"type": "math", "data": {"operation": "add", "a": 10, "b": 20}},
        {"type": "slow", "data": "Slow task"},
    ]
    
    for i in range(10):
        task_data = {
            "priority": i % 3,
            "payload": task_types[i % len(task_types)]
        }
        response = requests.post(f"{BASE_URL}/api/v1/tasks", json=task_data)
        if response.status_code == 200:
            print(f"   ✅ Submitted task {i+1}/10")
        else:
            print(f"   ❌ Failed to submit task {i+1}")
    
    # Wait for processing
    print("\n5. Waiting for task processing...")
    time.sleep(3)
    
    # Check updated metrics
    print("\n6. Checking Updated Metrics...")
    response = requests.get(f"{BASE_URL}/api/v1/metrics")
    if response.status_code == 200:
        metrics = response.json()
        print("📊 Updated Metrics:")
        print(f"   Tasks Submitted: {metrics['counters']['tasks_submitted']}")
        print(f"   Tasks Completed: {metrics['counters']['tasks_completed']}")
        print(f"   Tasks Failed: {metrics['counters']['tasks_failed']}")
        print(f"   Throughput: {metrics['rates']['throughput_rate']:.2f} tasks/sec")
        
        if metrics.get('processing_times'):
            pt = metrics['processing_times']
            print(f"\n⏱️  Processing Times:")
            print(f"   Average: {pt['avg']*1000:.1f}ms")
            print(f"   P95: {pt['p95']*1000:.1f}ms")
            print(f"   P99: {pt['p99']*1000:.1f}ms")
    
    print("\n" + "=" * 50)
    print("✅ Monitoring test completed!")
    print("\nTo see the live dashboard, run:")
    print("  python distributed_queue/monitoring/dashboard.py")

if __name__ == "__main__":
    try:
        test_monitoring()
    except requests.exceptions.ConnectionError:
        print("❌ Error: Could not connect to API server")
        print("Make sure the server is running: python run_api.py")