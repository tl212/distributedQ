"""
Redis backend for keeping tasks safe and sound
This module gives you a Redis-powered storage system for your distributed queue.
It handles the important stuff - making sure tasks don't disappear, letting multiple
workers process them across different machines, and helping everything recover
gracefully when things go wrong.
"""

import json
import time
import redis
import pickle
from typing import Optional, List, Dict, Any
from dataclasses import asdict
import logging

from ..core.queue import Task, TaskStatus

logger = logging.getLogger(__name__)


class RedisBackend:
    """
    Redis-based storage backend for the distributed queue    
    Uses Redis data structures:
    - Sorted Set for priority queue (queue:pending)
    - Hash for task details (tasks:details)
    - List for dead letter queue (queue:dlq)
    - Hash for task locks (tasks:locks)
    """
    
    # redis key prefixes
    QUEUE_PENDING = "queue:pending"        # sorted set of pending tasks
    QUEUE_PROCESSING = "queue:processing"  # set of tasks being processed
    QUEUE_DELAYED = "queue:delayed"        # sorted set of delayed tasks
    QUEUE_DLQ = "queue:dlq"                # list of dead letter tasks
    TASKS_DETAILS = "tasks:details"       # hash of task details
    TASKS_LOCKS = "tasks:locks"           # hash of task locks
    METRICS_KEY = "metrics"                # hash for metrics data
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0", 
                 key_prefix: str = "distqueue",
                 serialization: str = "json"):
        """
        Initialize Redis backend
        Args: 
        - redis_url: Redis connection URL
        - key_prefix: Prefix for all Redis keys (for namespacing)
        - serialization: Serialization method ('json' or 'pickle')
        """
        self.client = redis.Redis.from_url(redis_url, decode_responses=False)
        self.key_prefix = key_prefix
        self.serialization = serialization
        
        # test connection
        try:
            self.client.ping()
            logger.info(f"Connected to Redis at {redis_url}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _key(self, key: str) -> str:
        # generate namespaced redis key
        return f"{self.key_prefix}:{key}"
    
    def _serialize_task(self, task: Task) -> bytes:
        # serialize a task for storage
        task_dict = {
            'task_id': task.task_id,
            'priority': task.priority,
            'payload': task.payload,
            'created_at': task.created_at,
            'execute_after': task.execute_after,
            'retry_count': task.retry_count,
            'max_retries': task.max_retries,
            'status': task.status.value
        }
        
        if self.serialization == "json":
            return json.dumps(task_dict).encode('utf-8')
        else:
            return pickle.dumps(task_dict)
    
    def _deserialize_task(self, data: bytes) -> Task:
        # deserialize a task from storage
        if self.serialization == "json":
            task_dict = json.loads(data.decode('utf-8'))
        else:
            task_dict = pickle.loads(data)
        
        task = Task(
            priority=task_dict['priority'],
            payload=task_dict['payload'],
            max_retries=task_dict.get('max_retries', 3)
        )
        task.task_id = task_dict['task_id']
        task.created_at = task_dict['created_at']
        task.execute_after = task_dict.get('execute_after')
        task.retry_count = task_dict.get('retry_count', 0)
        task.status = TaskStatus(task_dict.get('status', 'pending'))
        
        return task
    
    def put_task(self, task: Task) -> bool:
        """
        Add a task to the queue
        Args: task: Task to add
        Returns: True if successful
        """
        try:
            # serialize task
            task_data = self._serialize_task(task)
            
            # store task details
            self.client.hset(
                self._key(self.TASKS_DETAILS),
                task.task_id,
                task_data
            )
            
            # add to appropriate queue based on delay
            if task.execute_after and task.execute_after > time.time():
                # add to delayed queue with execute_after as score
                self.client.zadd(
                    self._key(self.QUEUE_DELAYED),
                    {task.task_id: task.execute_after}
                )
            else:
                # add to pending queue with priority as score (lower = higher priority)
                self.client.zadd(
                    self._key(self.QUEUE_PENDING),
                    {task.task_id: task.priority}
                )
            
            logger.debug(f"Added task {task.task_id} to Redis")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add task to Redis: {e}")
            return False
    
    def get_task(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Get the highest priority task that's ready for execution
        Args:
        - block: Whether to block waiting for a task
        - timeout: Maximum time to wait (seconds)
        Returns: Task if available, None otherwise
        """
        start_time = time.time()
        
        while True:
            # first, check for delayed tasks that are now ready
            self._process_delayed_tasks()
            
            # try to get a task from the pending queue
            with self.client.pipeline() as pipe:
                try:
                    # use ZPOPMIN to atomically get and remove lowest score (highest priority)
                    pipe.watch(self._key(self.QUEUE_PENDING))
                    result = pipe.zpopmin(self._key(self.QUEUE_PENDING), 1)
                    
                    if result:
                        task_id = result[0][0].decode('utf-8') if isinstance(result[0][0], bytes) else result[0][0]
                        
                        # get task details
                        task_data = self.client.hget(self._key(self.TASKS_DETAILS), task_id)
                        if task_data:
                            task = self._deserialize_task(task_data)
                            
                            # mark as processing
                            self.client.sadd(self._key(self.QUEUE_PROCESSING), task_id)
                            task.status = TaskStatus.PROCESSING
                            
                            # update task in storage
                            self.client.hset(
                                self._key(self.TASKS_DETAILS),
                                task.task_id,
                                self._serialize_task(task)
                            )
                            
                            return task
                    
                except redis.WatchError:
                    # another client modified the queue, retry
                    continue
            
            # no task available
            if not block:
                return None
            
            # check timeout
            if timeout and (time.time() - start_time) >= timeout:
                return None
            
            # wait before retrying
            time.sleep(0.1)
    
    def _process_delayed_tasks(self):
        # move delayed tasks that are ready to the pending queue
        current_time = time.time()
        
        # get delayed tasks that are ready (score <= current_time)
        ready_tasks = self.client.zrangebyscore(
            self._key(self.QUEUE_DELAYED),
            0, current_time
        )
        
        if ready_tasks:
            with self.client.pipeline() as pipe:
                for task_id_bytes in ready_tasks:
                    task_id = task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else task_id_bytes
                    
                    # get task details to get priority
                    task_data = self.client.hget(self._key(self.TASKS_DETAILS), task_id)
                    if task_data:
                        task = self._deserialize_task(task_data)
                        
                        # move from delayed to pending queue
                        pipe.zrem(self._key(self.QUEUE_DELAYED), task_id)
                        pipe.zadd(self._key(self.QUEUE_PENDING), {task_id: task.priority})
                
                pipe.execute()
    
    def mark_completed(self, task_id: str) -> bool:
        # mark a task as completed and remove from queues
        try:
            # remove from processing set
            self.client.srem(self._key(self.QUEUE_PROCESSING), task_id)
            
            # udate task status
            task_data = self.client.hget(self._key(self.TASKS_DETAILS), task_id)
            if task_data:
                task = self._deserialize_task(task_data)
                task.status = TaskStatus.COMPLETED
                
                # as an option keep completed tasks for a while (e.g., 1 hour)
                self.client.hset(
                    self._key(self.TASKS_DETAILS),
                    task_id,
                    self._serialize_task(task)
                )
                self.client.expire(self._key(f"task:{task_id}"), 3600)  # Expire after 1 hour
                
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to mark task {task_id} as completed: {e}")
            return False
    
    def mark_failed(self, task_id: str, error: Optional[str] = None) -> bool:
        # mark a task as failed and handle retry/DLQ logic
        try:
            # get task details
            task_data = self.client.hget(self._key(self.TASKS_DETAILS), task_id)
            if not task_data:
                return False
            
            task = self._deserialize_task(task_data)
            task.retry_count += 1
            
            # remove from processing set
            self.client.srem(self._key(self.QUEUE_PROCESSING), task_id)
            
            if task.retry_count >= task.max_retries:
                # move to dead letter queue
                task.status = TaskStatus.DEAD
                self.client.lpush(self._key(self.QUEUE_DLQ), task_id)
                logger.info(f"Task {task_id} moved to dead letter queue after {task.retry_count} retries")
            else:
                # requeue with exponential backoff
                task.status = TaskStatus.PENDING
                task.execute_after = time.time() + (2 ** task.retry_count)
                
                # add back to delayed queue
                self.client.zadd(
                    self._key(self.QUEUE_DELAYED),
                    {task_id: task.execute_after}
                )
                logger.info(f"Task {task_id} requeued for retry {task.retry_count}/{task.max_retries}")
            
            # update task details
            self.client.hset(
                self._key(self.TASKS_DETAILS),
                task_id,
                self._serialize_task(task)
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark task {task_id} as failed: {e}")
            return False
    
    def get_dead_letters(self) -> List[Task]:
        # get all tasks in the dead letter queue
        try:
            task_ids = self.client.lrange(self._key(self.QUEUE_DLQ), 0, -1)
            tasks = []
            
            for task_id_bytes in task_ids:
                task_id = task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else task_id_bytes
                task_data = self.client.hget(self._key(self.TASKS_DETAILS), task_id)
                if task_data:
                    tasks.append(self._deserialize_task(task_data))
            
            return tasks
            
        except Exception as e:
            logger.error(f"Failed to get dead letter tasks: {e}")
            return []
    
    def size(self) -> int:
        # get the total number of pending tasks
        try:
            pending = self.client.zcard(self._key(self.QUEUE_PENDING))
            delayed = self.client.zcard(self._key(self.QUEUE_DELAYED))
            return pending + delayed
        except Exception as e:
            logger.error(f"Failed to get queue size: {e}")
            return 0
    
    def clear(self):
        # clear all tasks from all queues
        try:
            # get all keys with our prefix
            pattern = f"{self.key_prefix}:*"
            keys = self.client.keys(pattern)
            
            if keys:
                self.client.delete(*keys)
                logger.info(f"Cleared {len(keys)} Redis keys")
            
        except Exception as e:
            logger.error(f"Failed to clear Redis queues: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        # get queue statistics from Redis
        try:
            return {
                'pending': self.client.zcard(self._key(self.QUEUE_PENDING)),
                'processing': self.client.scard(self._key(self.QUEUE_PROCESSING)),
                'delayed': self.client.zcard(self._key(self.QUEUE_DELAYED)),
                'dead_letters': self.client.llen(self._key(self.QUEUE_DLQ)),
                'total_tasks': self.client.hlen(self._key(self.TASKS_DETAILS))
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
    
    def recover_stale_tasks(self, timeout: int = 300):
        """
        Recover tasks that have been processing for too long
        Args:
        - timeout: Time in seconds after which a task is considered stale
        """
        try:
            processing_tasks = self.client.smembers(self._key(self.QUEUE_PROCESSING))
            current_time = time.time()
            
            for task_id_bytes in processing_tasks:
                task_id = task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else task_id_bytes
                
                # check task processing time (would need to track this separately)
                # for now, we'll just re-queue old tasks
                task_data = self.client.hget(self._key(self.TASKS_DETAILS), task_id)
                if task_data:
                    task = self._deserialize_task(task_data)
                    
                    # simple check: if task is old, requeue it
                    if current_time - task.created_at > timeout:
                        logger.warning(f"Recovering stale task {task_id}")
                        
                        # remove from processing and add back to pending
                        self.client.srem(self._key(self.QUEUE_PROCESSING), task_id)
                        self.client.zadd(self._key(self.QUEUE_PENDING), {task_id: task.priority})
                        
        except Exception as e:
            logger.error(f"Failed to recover stale tasks: {e}")