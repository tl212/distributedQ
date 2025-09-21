"""
Basic Usage Example for Distributed Queue

This example demonstrates:
- Creating a priority queue
- Adding tasks with different priorities
- Processing tasks with workers
- Handling task failures and retries
"""

import sys
import time
import random
from pathlib import Path

# add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from distributed_queue.core.queue import PriorityQueue, Task
from distributed_queue.core.worker import Worker, WorkerConfig, WorkerPool

# simple task handler
def simple_task_handler(data):
    print(f"Processing: {data}")
    time.sleep(random.uniform(0.5, 2.0))
    return f"Completed: {data}"

# failing task handler
def failing_task_handler(data):
    if random.random() < 0.3:
        raise Exception(f"Random failure for: {data}")
    return f"Success: {data}"


# math task handler
def math_task_handler(data):
    operation = data.get('operation')
    a = data.get('a')
    b = data.get('b')
    
    if operation == 'add':
        return a + b
    elif operation == 'multiply':
        return a * b
    elif operation == 'divide':
        if b == 0:
            raise ValueError("Division by zero")
        return a / b
    else:
        raise ValueError(f"Unknown operation: {operation}")


# error callback
def error_callback(task: Task, error: Exception):
    print(f"Task {task.task_id} failed with error: {error}")


# main function
def main():
    print("=" * 50)
    print("Distributed Queue Basic Usage Example")
    print("=" * 50)
    
    # create a priority queue
    queue = PriorityQueue(max_size=100, rate_limit=10)
    
    # create worker pool
    pool = WorkerPool(queue)
    
    # add workers with different configurations
    worker1_config = WorkerConfig(
        name="Worker-1",
        max_workers=3,
        error_handler=error_callback
    )
    worker1 = pool.add_worker(worker1_config)
    
    worker2_config = WorkerConfig(
        name="Worker-2",
        max_workers=2,
        error_handler=error_callback
    )
    worker2 = pool.add_worker(worker2_config)
    
    # register handlers for different task types
    for worker in [worker1, worker2]:
        worker.register_handler("simple", simple_task_handler)
        worker.register_handler("failing", failing_task_handler)
        worker.register_handler("math", math_task_handler)
    
    print("\n--- Adding Tasks to Queue ---")
    
    # add simple tasks with different priorities
    for i in range(5):
        task = Task(
            priority=i,  # lower number = higher priority
            payload={
                'type': 'simple',
                'data': f'Task-{i}'
            }
        )
        queue.put(task)
        print(f"Added task with priority {i}: Task-{i}")
    
    # add math tasks
    math_operations = [
        {'operation': 'add', 'a': 10, 'b': 5},
        {'operation': 'multiply', 'a': 7, 'b': 8},
        {'operation': 'divide', 'a': 100, 'b': 4},
    ]
    
    for i, op in enumerate(math_operations):
        task = Task(
            priority=1,  # High priority
            payload={
                'type': 'math',
                'data': op
            }
        )
        queue.put(task)
        print(f"Added math task: {op}")
    
    # add some tasks that might fail
    for i in range(3):
        task = Task(
            priority=5,  # lower priority
            payload={
                'type': 'failing',
                'data': f'Risky-Task-{i}'
            },
            max_retries=2  # will retry twice before going to DLQ
        )
        queue.put(task)
        print(f"Added risky task: Risky-Task-{i}")
    
    # add a delayed task
    delayed_task = Task(
        priority=0,  # highest priority
        payload={
            'type': 'simple',
            'data': 'Delayed Task (executes after 5 seconds)'
        },
        execute_after=time.time() + 5  # execute 5 seconds from now
    )
    queue.put(delayed_task)
    print("Added delayed task (will execute in 5 seconds)")
    
    print(f"\nQueue size: {queue.size()}")
    
    # start all workers
    print("\n--- Starting Workers ---")
    pool.start_all()
    
    # monitor progress
    print("\n--- Processing Tasks ---")
    print("Workers are processing tasks...")
    
    start_time = time.time()
    while queue.size() > 0 or any(w.get_stats()['active_tasks'] > 0 for w in pool.workers.values()):
        time.sleep(1)
        stats = pool.get_stats()
        active_tasks = sum(w['active_tasks'] for w in stats['workers'])
        print(f"Queue size: {queue.size()}, Active tasks: {active_tasks}")
        
        # timeout after 30 seconds
        if time.time() - start_time > 30:
            print("Timeout reached, stopping...")
            break
    
    # wait a bit more to ensure completion
    time.sleep(2)
    
    # check dead letter queue
    dead_letters = queue.get_dead_letters()
    if dead_letters:
        print(f"\n--- Dead Letter Queue ({len(dead_letters)} tasks) ---")
        for task in dead_letters:
            print(f"Failed task {task.task_id}: {task.payload}")
    
    # stop workers
    print("\n--- Stopping Workers ---")
    pool.stop_all(wait=True)
    
    # final statistics
    print("\n--- Final Statistics ---")
    final_stats = pool.get_stats()
    print(f"Total workers: {final_stats['total_workers']}")
    for worker_stats in final_stats['workers']:
        print(f"  {worker_stats['name']}: Processed tasks successfully")
    
    print(f"\nTotal tasks processed: {11 - queue.size()}")
    print(f"Tasks in dead letter queue: {len(dead_letters)}")
    
    print("\n✅ Example completed successfully!")


if __name__ == "__main__":
    main()