"""
Priority Queue Implementation for Distributed Task Queue System

This module implements a thread-safe priority queue with custom heap operations,
supporting task prioritization, delayed execution, and dead letter queue handling.
"""

import heapq
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, List, Callable
from uuid import uuid4
from datetime import datetime, timedelta
from enum import Enum


class TaskStatus(Enum):
    """Task execution status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"  # Moved to dead letter queue


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
        self._task_map: Dict[str, Task] = {}  # For O(1) task lookup
        
    def put(self, task: Task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Add a task to the queue
        
        Args:
            task: Task to add
            block: Block if queue is full
            timeout: Maximum time to wait
            
        Returns:
            True if task was added, False otherwise
        """
        with self._not_full:
            # Handle backpressure
            if self._max_size is not None:
                while len(self._heap) >= self._max_size:
                    if not block:
                        return False
                    if not self._not_full.wait(timeout):
                        return False
            
            # Add task to heap
            heapq.heappush(self._heap, task)
            self._task_map[task.task_id] = task
            self._not_empty.notify()
            return True
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Get the highest priority task that's ready for execution
        
        Args:
            block: Block if queue is empty
            timeout: Maximum time to wait
            
        Returns:
            Task if available, None otherwise
        """
        with self._not_empty:
            # Rate limiting
            if self._rate_limit is not None:
                elapsed = time.time() - self._last_get_time
                min_interval = 1.0 / self._rate_limit
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)
            
            while True:
                # Remove any delayed tasks not yet ready
                current_time = time.time()
                ready_tasks = []
                
                while self._heap:
                    task = self._heap[0]
                    if task.execute_after and task.execute_after > current_time:
                        break
                    ready_tasks.append(heapq.heappop(self._heap))
                
                # Put back delayed tasks
                for task in ready_tasks:
                    if task.execute_after and task.execute_after > current_time:
                        heapq.heappush(self._heap, task)
                    else:
                        # Found a ready task
                        task.status = TaskStatus.PROCESSING
                        self._last_get_time = time.time()
                        self._not_full.notify()
                        return task
                
                # No ready tasks
                if not block:
                    return None
                if not self._not_empty.wait(timeout):
                    return None
    
    def mark_completed(self, task_id: str) -> bool:
        """Mark a task as completed"""
        with self._lock:
            if task_id in self._task_map:
                self._task_map[task_id].status = TaskStatus.COMPLETED
                del self._task_map[task_id]
                return True
            return False
    
    def mark_failed(self, task_id: str, error: Optional[str] = None) -> bool:
        """
        Mark a task as failed and potentially move to dead letter queue
        
        Args:
            task_id: Task identifier
            error: Error message
            
        Returns:
            True if task was marked as failed
        """
        with self._lock:
            if task_id not in self._task_map:
                return False
            
            task = self._task_map[task_id]
            task.retry_count += 1
            
            if task.retry_count >= task.max_retries:
                # Move to dead letter queue
                task.status = TaskStatus.DEAD
                self._dead_letter_queue.append(task)
                del self._task_map[task_id]
            else:
                # Requeue with exponential backoff
                task.status = TaskStatus.PENDING
                task.execute_after = time.time() + (2 ** task.retry_count)
                heapq.heappush(self._heap, task)
            
            return True
    
    def get_dead_letters(self) -> List[Task]:
        """Get all tasks in the dead letter queue"""
        with self._lock:
            return self._dead_letter_queue.copy()
    
    def size(self) -> int:
        """Get current queue size"""
        with self._lock:
            return len(self._heap)
    
    def clear(self):
        """Clear all tasks from the queue"""
        with self._lock:
            self._heap.clear()
            self._task_map.clear()
            self._dead_letter_queue.clear()