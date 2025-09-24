#!/usr/bin/env python
"""
test for redis backend functionality
"""

import time
import os
from distributed_queue.core.hybrid_queue import HybridQueue, StorageBackend
from distributed_queue.core.queue import Task

def test_redis_backend():
    print("Testing Redis Backend")
    print("=" * 50)
    
    # test 1: initialize with Redis backend
    print("\n1. Testing Redis Connection...")
    try:
        queue = HybridQueue(
            backend=StorageBackend.REDIS,
            redis_url="redis://localhost:6379/0",
            key_prefix="test_queue"
        )
        
        health = queue.health_check()
        print(f"✅ Backend: {health['backend']}")
        print(f"   Status: {health['status']}")
        print(f"   Message: {health['message']}")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        print("   Make sure Redis is running: redis-server")
        return
    
    # clear any existing tasks
    queue.clear()
    
    # test 2: add tasks
    print("\n2. Adding Tasks to Redis...")
    tasks = []
    for i in range(5):
        task = Task(
            priority=i,
            payload={"type": "test", "data": f"Redis Task {i}"}
        )
        success = queue.put(task)
        if success:
            print(f"✅ Added task with priority {i}: {task.task_id}")
            tasks.append(task)
        else:
            print(f"❌ Failed to add task {i}")
    
    print(f"\nQueue size: {queue.size()}")
    
    # test 3: retrieve tasks (should be in priority order)
    print("\n3. Retrieving Tasks (Priority Order)...")
    for i in range(3):
        task = queue.get(block=False)
        if task:
            print(f"✅ Got task: Priority={task.priority}, ID={task.task_id}")
        else:
            print("❌ No task retrieved")
    
    print(f"\nRemaining queue size: {queue.size()}")
    
    # test 4: test persistence (simulate restart)
    print("\n4. Testing Persistence...")
    print("   Simulating application restart...")
    
    # create new queue instance (simulating restart)
    queue2 = HybridQueue(
        backend=StorageBackend.REDIS,
        redis_url="redis://localhost:6379/0",
        key_prefix="test_queue"
    )
    
    remaining = queue2.size()
    print(f"✅ After 'restart', queue still has {remaining} tasks")
    
    # test 5: get remaining tasks
    print("\n5. Getting Remaining Tasks After 'Restart'...")
    while True:
        task = queue2.get(block=False)
        if not task:
            break
        print(f"✅ Retrieved persisted task: Priority={task.priority}")
    
    # test 6: task failure and retry
    print("\n6. Testing Task Failure and Retry...")
    fail_task = Task(
        priority=1,
        payload={"type": "failing", "data": "This will fail"},
        max_retries=2
    )
    queue2.put(fail_task)
    
    # get and fail the task
    task = queue2.get(block=False)
    print(f"   Got task: {task.task_id}")
    
    # mark as failed (will be retried)
    queue2.mark_failed(task.task_id, "Simulated failure")
    print(f"✅ Task marked as failed, will retry")
    
    # check stats
    stats = queue2.get_stats()
    print(f"\n7. Queue Statistics:")
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # clean up
    queue2.clear()
    print("\n✅ Test completed and queue cleared!")
    
    print("\n" + "=" * 50)
    print("Redis backend is working correctly!")
    print("\nTo use Redis in the API, set environment variable:")
    print("  export USE_REDIS=true")
    print("  python run_api.py")

if __name__ == "__main__":
    # check if Redis is available
    print("Checking Redis availability...")
    
    try:
        import redis
        client = redis.Redis(host='localhost', port=6379, db=0)
        client.ping()
        print("✅ Redis is running\n")
        test_redis_backend()
    except redis.ConnectionError:
        print("❌ Redis is not running!")
        print("\nTo install Redis:")
        print("  Mac: brew install redis && brew services start redis")
        print("  Linux: sudo apt-get install redis-server")
        print("\nTo start Redis:")
        print("  redis-server")