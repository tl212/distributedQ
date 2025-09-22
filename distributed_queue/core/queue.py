"""
Priority Queue for our distributed task system

This is the main priority queue implementation - it's thread-safe (finally!) 
and handles all the task scheduling stuff we need. Supports priorities, 
delayed tasks, and has DLQ handling for when things go wrong.

Note: Custom heap ops because we needed to support task rescheduling
TODO: might want to add metrics for queue depth monitoring
"""

import heapq
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, List, Callable
from uuid import uuid4
from datetime import datetime, timedelta
from enum import Enum
from ..monitoring.metrics import get_metrics

# task executon status
class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead" 


@dataclass(order=True)
class Task:
    """
    Represents a task in the queue
    
    Attributes:
        priority: Task priority (lower values = higher priority)
        payload: The actual task data
        task_id: Unique task identifier
        created_at: Task creation timestamp
        execute_after: Delayed execution timestamp
        retry_count: Number of retry attempts
        max_retries: Maximum allowed retries
    """
    priority: int
    payload: Any = field(compare=False)
    task_id: str = field(default_factory=lambda: str(uuid4()), compare=False)
    created_at: float = field(default_factory=time.time, compare=False)
    execute_after: Optional[float] = field(default=None, compare=False)
    retry_count: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    status: TaskStatus = field(default=TaskStatus.PENDING, compare=False)
    

class PriorityQueue:
    """
    Thread-safe priority queue implementation with advanced features
    
    Features:
        - Priority-based task ordering
        - Delayed task execution
        - Dead letter queue for failed tasks
        - Rate limiting support
        - Backpressure handling
    """
    
    def __init__(self, max_size: Optional[int] = None, rate_limit: Optional[int] = None):
        """
        Initialize the priority queue
        
        Args:
            max_size: Maximum queue size (None for unlimited)
            rate_limit: Maximum tasks per second (None for unlimited)
        """
        self._heap: List[Task] = []
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._dead_letter_queue: List[Task] = []
        self._max_size = max_size
        self._rate_limit = rate_limit
        self._last_get_time = 0.0
        self._task_map: Dict[str, Task] = {}  # required for O(1) task lookup
        
    def put(self, task: Task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Add a task to the queue
        
        Args:
            task: Task to add
            block: Block if queue is full
            timeout: Maximum time to wait
            
        Returns: true if task was added, false otherwise
        """
        with self._not_full:
            # handle backpressure - if the queue is full, we need to block or return false
            if self._max_size is not None:
                while len(self._heap) >= self._max_size:
                    if not block:
                        return False
                    if not self._not_full.wait(timeout):
                        return False
            
            # add task to heap
            heapq.heappush(self._heap, task)
            self._task_map[task.task_id] = task
            self._not_empty.notify()
            
            # record metrics
            metrics = get_metrics()
            task_type = "default"
            if isinstance(task.payload, dict):
                task_type = task.payload.get('type', 'default')
            metrics.record_task_submitted(task_type, task.priority)
            
            return True
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Task]:
        """
        get the highest priority task that's ready for execution
        
        Args:
            block: Block if queue is empty
            timeout: Maximum time to wait
            
        Returns: task if available, none otherwise
        """
        with self._not_empty:
            # rate limiting
            if self._rate_limit is not None:
                elapsed = time.time() - self._last_get_time
                min_interval = 1.0 / self._rate_limit
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)
            
            while True:
                current_time = time.time()
                
                # check if we have any tasks
                if not self._heap:
                    if not block:
                        return None
                    if not self._not_empty.wait(timeout):
                        return None
                    continue
                
                # check the top task
                task = self._heap[0]
                
                # if it's delayed and not ready yet, wait or return
                if task.execute_after and task.execute_after > current_time:
                    if not block:
                        return None
                    # wait until the task is ready or timeout
                    wait_time = min(task.execute_after - current_time, timeout) if timeout else task.execute_after - current_time
                    if not self._not_empty.wait(wait_time):
                        return None
                    continue
                
                # task is ready, remove and return it
                task = heapq.heappop(self._heap)
                task.status = TaskStatus.PROCESSING
                self._last_get_time = time.time()
                self._not_full.notify()
                return task
    
    def mark_completed(self, task_id: str) -> bool:
        """mark a task as completed"""
        with self._lock:
            if task_id in self._task_map:
                self._task_map[task_id].status = TaskStatus.COMPLETED
                del self._task_map[task_id]
                return True
            return False
    
    def mark_failed(self, task_id: str, error: Optional[str] = None) -> bool:
        """
        mark a task as failed and potentially move to dead letter queue
        
        Args:
            task_id: Task identifier
            error: Error message
            
        Returns: true if task was marked as failed
        """
        with self._lock:
            if task_id not in self._task_map:
                return False
            
            task = self._task_map[task_id]
            task.retry_count += 1
            
            if task.retry_count >= task.max_retries:
                # move to dead letter queue
                task.status = TaskStatus.DEAD
                self._dead_letter_queue.append(task)
                del self._task_map[task_id]
            else:
                # requeue with exponential backoff
                task.status = TaskStatus.PENDING
                task.execute_after = time.time() + (2 ** task.retry_count)
                heapq.heappush(self._heap, task)
            
            return True
    
    def get_dead_letters(self) -> List[Task]:
        """get all tasks in the dead letter queue"""
        with self._lock:
            return self._dead_letter_queue.copy()
    
    def size(self) -> int:
        """get current queue size"""
        with self._lock:
            return len(self._heap)
    
    def clear(self):
        """clear all tasks from the queue"""
        with self._lock:
            self._heap.clear()
            self._task_map.clear()
            self._dead_letter_queue.clear()