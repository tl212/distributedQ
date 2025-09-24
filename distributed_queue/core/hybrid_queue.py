"""
Hybrid queue that works with both in-memory and Redis storage
This will give one consistent interface that can flip between different storage
options depending on the setup. Perfect for when building locally
and when running in production.
"""

import logging
from typing import Optional, List, Any
from enum import Enum

from .queue import PriorityQueue, Task, TaskStatus
from ..storage.redis_backend import RedisBackend
from ..monitoring.metrics import get_metrics

logger = logging.getLogger(__name__)


class StorageBackend(Enum):
    # available storage backends
    MEMORY = "memory"
    REDIS = "redis"


class HybridQueue:
    """
    Queue implementation that can use either in-memory or Redis storage
    This provides seamless switching between backends while maintaining
    the same interface for the application.
    """
    
    def __init__(
        self,
        backend: StorageBackend = StorageBackend.MEMORY,
        redis_url: Optional[str] = "redis://localhost:6379/0",
        max_size: Optional[int] = None,
        rate_limit: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize the hybrid queue
        
        Args:
        - backend: storage backend to use
        - redis_url: redis connection URL (if using Redis)
        - max_size: maximum queue size (for in-memory)
        - rate_limit: rate limit for task processing
        - **kwargs: additional backend-specific arguments
        """
        self.backend_type = backend
        self.metrics = get_metrics()
        
        if backend == StorageBackend.REDIS:
            try:
                # try to initialize Redis backend
                self.backend = RedisBackend(
                    redis_url=redis_url,
                    key_prefix=kwargs.get('key_prefix', 'distqueue'),
                    serialization=kwargs.get('serialization', 'json')
                )
                self.is_redis = True
                logger.info("Using Redis backend for queue storage")
            except Exception as e:
                logger.warning(f"Failed to connect to Redis: {e}. Falling back to in-memory storage.")
                self._init_memory_backend(max_size, rate_limit)
        else:
            self._init_memory_backend(max_size, rate_limit)
    
    def _init_memory_backend(self, max_size: Optional[int], rate_limit: Optional[int]):
        # initialize in-memory backend
        self.backend = PriorityQueue(max_size=max_size, rate_limit=rate_limit)
        self.is_redis = False
        logger.info("Using in-memory backend for queue storage")
    
    def put(self, task: Task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Add a task to the queue
        Args:
        - task: task to add
        - block: whether to block if queue is full (memory only)
        - timeout: timeout for blocking (memory only)
        Returns: True if successful
        """
        if self.is_redis:
            success = self.backend.put_task(task)
        else:
            success = self.backend.put(task, block=block, timeout=timeout)
        
        if success:
            # Update metrics
            task_type = "default"
            if isinstance(task.payload, dict):
                task_type = task.payload.get('type', 'default')
            self.metrics.record_task_submitted(task_type, task.priority)
        
        return success
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Get the highest priority task
        Args:
        - block: Whether to block waiting for a task
        - timeout: Maximum time to wait
        Returns: Task if available, None otherwise
        """
        if self.is_redis:
            task = self.backend.get_task(block=block, timeout=timeout)
        else:
            task = self.backend.get(block=block, timeout=timeout)
        
        if task:
            # update metrics
            self.metrics.record_task_started()
            self.metrics.update_queue_depth(self.size())
        
        return task
    
    def mark_completed(self, task_id: str) -> bool:
        # mark a task as completed
        if self.is_redis:
            success = self.backend.mark_completed(task_id)
        else:
            success = self.backend.mark_completed(task_id)
        
        if success:
            self.metrics.update_queue_depth(self.size())
        
        return success
    
    def mark_failed(self, task_id: str, error: Optional[str] = None) -> bool:
        # mrk a task as failed
        if self.is_redis:
            return self.backend.mark_failed(task_id, error)
        else:
            return self.backend.mark_failed(task_id, error)
    
    def get_dead_letters(self) -> List[Task]:
        # get all tasks in dead letter queue
        if self.is_redis:
            return self.backend.get_dead_letters()
        else:
            return self.backend.get_dead_letters()
    
    def size(self) -> int:
        # get current queue size
        if self.is_redis:
            return self.backend.size()
        else:
            return self.backend.size()
    
    def clear(self):
        # clear all tasks
        if self.is_redis:
            self.backend.clear()
        else:
            self.backend.clear()
    
    def get_stats(self) -> dict:
        # get queue statistics
        if self.is_redis:
            stats = self.backend.get_stats()
            stats['backend'] = 'redis'
        else:
            stats = {
                'pending': self.backend.size(),
                'processing': len([t for t in self.backend._task_map.values() 
                                 if t.status == TaskStatus.PROCESSING]),
                'dead_letters': len(self.backend.get_dead_letters()),
                'backend': 'memory'
            }
        return stats
    
    def recover_stale_tasks(self, timeout: int = 300):
        # recover tasks that have been processing too long (Redis backend only)
        if self.is_redis:
            self.backend.recover_stale_tasks(timeout)
            logger.info(f"Recovered stale tasks older than {timeout} seconds")
        else:
            logger.debug("Task recovery not needed for in-memory backend")
    
    def health_check(self) -> dict:
        # check queue health
        try:
            if self.is_redis:
                # test redis connection
                self.backend.client.ping()
                status = "healthy"
                message = "Redis backend is operational"
            else:
                status = "healthy"
                message = "In-memory backend is operational"
            
            return {
                'status': status,
                'backend': 'redis' if self.is_redis else 'memory',
                'message': message,
                'stats': self.get_stats()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'backend': 'redis' if self.is_redis else 'memory',
                'message': str(e),
                'stats': {}
            }