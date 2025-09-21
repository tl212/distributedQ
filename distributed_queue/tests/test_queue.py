"""
unit tests for PriorityQueue implementation
"""

import pytest
import time
import threading
from typing import List
from distributed_queue.core.queue import PriorityQueue, Task, TaskStatus

# test suite for PriorityQueue
class TestPriorityQueue:
    
    # test basic enqueue and dequeue operations
    def test_basic_put_and_get(self):

        queue = PriorityQueue()
        
        # add tasks
        task1 = Task(priority=2, payload="Task 1")
        task2 = Task(priority=1, payload="Task 2")
        task3 = Task(priority=3, payload="Task 3")
        
        assert queue.put(task1)
        assert queue.put(task2)
        assert queue.put(task3)
        
        # verify size
        assert queue.size() == 3
        
        # get tasks (should be priority order: 1, 2, 3)
        retrieved1 = queue.get(block=False)
        assert retrieved1.priority == 1
        assert retrieved1.payload == "Task 2"
        assert retrieved1.status == TaskStatus.PROCESSING
        
        retrieved2 = queue.get(block=False)
        assert retrieved2.priority == 2
        assert retrieved2.payload == "Task 1"
        
        retrieved3 = queue.get(block=False)
        assert retrieved3.priority == 3
        assert retrieved3.payload == "Task 3"
        
        # queue should be empty
        assert queue.size() == 0
        assert queue.get(block=False) is None
    
    # test max size enforcement
    def test_max_size_enforcement(self):
        queue = PriorityQueue(max_size=2)
        
        task1 = Task(priority=1, payload="Task 1")
        task2 = Task(priority=2, payload="Task 2")
        task3 = Task(priority=3, payload="Task 3")
        
        assert queue.put(task1)
        assert queue.put(task2)
        
        # should fail as queue is full
        assert not queue.put(task3, block=False)
        
        # remove one and try again
        queue.get(block=False)
        assert queue.put(task3)
    
    # test delayed task execution
    def test_delayed_task_execution(self):
        queue = PriorityQueue()
        
        # add immediate task
        immediate = Task(priority=5, payload="Immediate")
        queue.put(immediate)
        
        # add delayed task with higher priority
        delayed = Task(
            priority=1,  # Higher priority
            payload="Delayed",
            execute_after=time.time() + 1.0  # 1 second delay
        )
        queue.put(delayed)
        
        # should get immediate task first despite lower priority
        retrieved = queue.get(block=False)
        assert retrieved.payload == "Immediate"
        
        # delayed task should not be available yet
        assert queue.get(block=False) is None
        
        # wait for delayed task to become ready
        time.sleep(1.1)
        retrieved = queue.get(block=False)
        assert retrieved.payload == "Delayed"
    
    # test task completion
    def test_task_completion(self):
        queue = PriorityQueue()
        
        task = Task(priority=1, payload="Test task")
        queue.put(task)
        
        retrieved = queue.get(block=False)
        assert retrieved.status == TaskStatus.PROCESSING
        
        # mark as completed
        assert queue.mark_completed(retrieved.task_id)
        
        # should not be able to mark again
        assert not queue.mark_completed(retrieved.task_id)
    
    # test task failure and retry logic
    def test_task_failure_and_retry(self):
        queue = PriorityQueue()
        
        task = Task(priority=1, payload="Failing task", max_retries=2)
        original_id = task.task_id
        queue.put(task)
        
        # get and fail the task
        retrieved = queue.get(block=False)
        assert queue.mark_failed(retrieved.task_id, "First failure")
        
        # task should be requeued with delay
        assert queue.size() == 1
        
        # wait for retry delay
        time.sleep(2.1)  # Exponential backoff: 2^1
        
        # get the retried task
        retrieved = queue.get(block=False)
        assert retrieved.task_id == original_id
        assert retrieved.retry_count == 1
        
        # fail again
        assert queue.mark_failed(retrieved.task_id, "Second failure")
        
        # should be moved to dead letter queue after max retries
        assert queue.size() == 0
        dead_letters = queue.get_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0].task_id == original_id
        assert dead_letters[0].status == TaskStatus.DEAD
    
    # test rate limiting functionality
    def test_rate_limiting(self):
        queue = PriorityQueue(rate_limit=5)  # 5 tasks per second
        
        # add multiple tasks
        for i in range(3):
            queue.put(Task(priority=i, payload=f"Task {i}"))
        
        start_time = time.time()
        
        # get tasks
        for _ in range(3):
            queue.get(block=False)
        
        elapsed = time.time() - start_time
        # should take at least 0.4 seconds (3 tasks at 5/sec = 0.6 sec minimum)
        assert elapsed >= 0.4
    
    def test_concurrent_access(self):
        # test thread-safe operations
        queue = PriorityQueue()
        results = []
        errors = []
        
        # producer
        def producer():
            try:
                for i in range(10):
                    task = Task(priority=i, payload=f"Task {i}")
                    queue.put(task)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        # consumer
        def consumer():
            try:
                for _ in range(10):
                    task = queue.get(block=True, timeout=2.0)
                    if task:
                        results.append(task.payload)
                        queue.mark_completed(task.task_id)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        # start concurrent threads
        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)
        
        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join()
        
        # check results
        assert len(errors) == 0
        assert len(results) == 10
        assert queue.size() == 0
    
    # test clearing the queue
    def test_clear_queue(self):
        queue = PriorityQueue()
        
        # add tasks
        for i in range(5):
            queue.put(Task(priority=i, payload=f"Task {i}"))
        
        # add failed task to DLQ
        task = Task(priority=0, payload="Failed", max_retries=0)
        queue.put(task)
        retrieved = queue.get(block=False)
        queue.mark_failed(retrieved.task_id)
        
        assert queue.size() == 5
        assert len(queue.get_dead_letters()) == 1
        
        # clear queue
        queue.clear()
        
        assert queue.size() == 0
        assert len(queue.get_dead_letters()) == 0
    
    # test blocking get with timeout
    def test_blocking_get_with_timeout(self):
        queue = PriorityQueue()
        
        start_time = time.time()
        result = queue.get(block=True, timeout=0.5)
        elapsed = time.time() - start_time
        
        assert result is None
        assert 0.4 <= elapsed <= 0.6  # allow some tolerance
    
    # test blocking put with timeout on full queue
    def test_blocking_put_with_timeout(self):
        queue = PriorityQueue(max_size=1)
        
        task1 = Task(priority=1, payload="Task 1")
        task2 = Task(priority=2, payload="Task 2")
        
        assert queue.put(task1)
        
        start_time = time.time()
        result = queue.put(task2, block=True, timeout=0.5)
        elapsed = time.time() - start_time
        
        assert not result
        assert 0.4 <= elapsed <= 0.6  # allow some tolerance

if __name__ == "__main__":
    pytest.main([__file__, "-v"])