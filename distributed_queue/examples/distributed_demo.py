"""
demonstration of distributed queue with redis backend
shows multiple workers processing tasks from shared queue
"""

import time
import threading
from config import QueueConfig, set_config
from distributed_queue.core.hybrid_queue import HybridQueue, StorageBackend
from distributed_queue.core.worker import Worker, WorkerConfig
from distributed_queue.core.queue import Task

def simulate_worker_node(node_id: int, queue: HybridQueue, num_tasks: int = 5):
    # simulate a worker node processing tasks
    print(f"\n[Node-{node_id}] Starting worker node...")
    
    # create worker for this node
    config = WorkerConfig(
        name=f"node-{node_id}-worker",
        max_workers=2,
        poll_interval=0.1
    )
    worker = Worker(queue, config)
    
    # register handler
    def process_task(data):
        print(f"[Node-{node_id}] Processing: {data}")
        time.sleep(0.5)  # simulate work
        return f"Completed by Node-{node_id}: {data}"
    
    worker.register_handler("distributed", process_task)
    worker.register_handler("default", process_task)
    
    # start worker
    worker.start()
    print(f"[Node-{node_id}] Worker started, processing tasks...")
    
    # process for a while
    time.sleep(num_tasks * 0.7)
    
    # stop worker
    worker.stop(wait=True)
    print(f"[Node-{node_id}] Worker stopped")

def main():
    print("=" * 60)
    print("DISTRIBUTED QUEUE DEMONSTRATION")
    print("=" * 60)
    
    # configure for Redis
    config = QueueConfig(
        use_redis=True,
        redis_url="redis://localhost:6379/0",
        redis_key_prefix="demo",
        max_queue_size=10000,
        rate_limit=100
    )
    set_config(config)
    
    # create shared queue (redis backend)
    print("\n1. Initializing Redis-backed Queue...")
    queue = HybridQueue(
        backend=StorageBackend.REDIS,
        redis_url=config.redis_url,
        key_prefix=config.redis_key_prefix
    )
    
    # clear any existing tasks
    queue.clear()
    
    health = queue.health_check()
    print(f"   Backend: {health['backend']}")
    print(f"   Status: {health['status']}")
    
    # add tasks to shared queue
    print("\n2. Adding 20 Tasks to Shared Queue...")
    for i in range(20):
        task = Task(
            priority=i % 3,  # vary priorities
            payload={
                "type": "distributed",
                "data": f"Task-{i:02d}"
            }
        )
        queue.put(task)
        print(f"   Added: Task-{i:02d} (Priority: {i % 3})")
    
    print(f"\n   Total tasks in queue: {queue.size()}")
    
    # simulate multiple worker nodes
    print("\n3. Starting 3 Worker Nodes (simulating distributed processing)...")
    print("   Each node will process tasks from the shared Redis queue")
    
    threads = []
    for node_id in range(1, 4):
        thread = threading.Thread(
            target=simulate_worker_node,
            args=(node_id, queue, 7)
        )
        threads.append(thread)
        thread.start()
        time.sleep(0.2)  # Stagger node starts
    
    # monitor progress
    print("\n4. Monitoring Distributed Processing...")
    start_time = time.time()
    
    while queue.size() > 0:
        stats = queue.get_stats()
        print(f"   Queue: {stats['pending']} pending, "
              f"{stats['processing']} processing")
        time.sleep(1)
        
        if time.time() - start_time > 30:  # Timeout after 30 seconds
            print("   Timeout reached")
            break
    
    # wait for all nodes to finish
    print("\n5. Waiting for Worker Nodes to Complete...")
    for thread in threads:
        thread.join()
    
    # final stats
    print("\n6. Final Statistics:")
    stats = queue.get_stats()
    print(f"   Pending: {stats['pending']}")
    print(f"   Processing: {stats['processing']}")
    print(f"   Dead Letters: {stats['dead_letters']}")
    print(f"   Elapsed Time: {time.time() - start_time:.1f} seconds")
    
    # demonstrate persistence
    print("\n7. Demonstrating Persistence...")
    remaining = queue.size()
    if remaining > 0:
        print(f"   {remaining} tasks remain in Redis after workers stopped")
        print("   These tasks persist and will be available when workers restart")
    else:
        print("   All tasks completed successfully!")
    
    print("\n" + "=" * 60)
    print("✅ Distributed queue demonstration completed!")
    print("\nKey Points Demonstrated:")
    print("- Multiple workers sharing same Redis queue")
    print("- Tasks distributed across worker nodes")
    print("- Persistence across worker restarts")
    print("- Priority-based processing")

if __name__ == "__main__":
    import sys
    
    # check Redis availability
    try:
        import redis
        client = redis.Redis(host='localhost', port=6379, db=0)
        client.ping()
        print("✅ Redis is running\n")
        main()
    except redis.ConnectionError:
        print("❌ Redis is not running!")
        print("\nTo start Redis with Docker:")
        print("  docker-compose up -d redis")
        print("\nOr install locally:")
        print("  Mac: brew install redis && brew services start redis")
        sys.exit(1)