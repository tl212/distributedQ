"""
Queue workers

Handles task consumption and execution from the main queue.
Based on the celery worker pattern but simplified for our use case.
"""

import threading
import time
import logging
from typing import Callable, Optional, Any, Dict
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from .queue import PriorityQueue, Task, TaskStatus

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
# configuration for worker instances
class WorkerConfig:  
    name: str
    max_workers: int = 5
    poll_interval: float = 0.1
    shutdown_timeout: float = 30.0
    error_handler: Optional[Callable[[Task, Exception], None]] = None


class Worker:
    """
    worker that processes tasks from a queue
    
    Features:
        - concurrent task processing with thread pool
        - graceful shutdown
        - error handling and retry logic
        - task execution callbacks
    """
    
    def __init__(self, queue: PriorityQueue, config: WorkerConfig):
        """
        initialize a worker
        
        Args:
            queue: the priority queue to consume tasks from
            config: Worker configuration
        """
        self.queue = queue
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self.handlers: Dict[str, Callable] = {}
        self.running = False
        self.worker_thread: Optional[threading.Thread] = None
        self.active_tasks: Dict[str, Future] = {}
        self._lock = threading.Lock()
        
    def register_handler(self, task_type: str, handler: Callable[[Any], Any]):
        """
        register a handler for a specific task type
        
        Args:
            task_type: type identifier for tasks
            handler: function to process tasks of this type
        """
        self.handlers[task_type] = handler
        logger.info(f"[{self.config.name}] Registered handler for task type: {task_type}")
        
    # start the worker    
    def start(self):
        if self.running:
            logger.warning(f"[{self.config.name}] Worker already running")
            return
            
        self.running = True
        self.worker_thread = threading.Thread(target=self._run, name=self.config.name)
        self.worker_thread.start()
        logger.info(f"[{self.config.name}] Worker started")
        
    def stop(self, wait: bool = True):
        """
        stop the worker
        
        Args:
            wait: whether to wait for active tasks to complete
        """
        if not self.running:
            return
            
        logger.info(f"[{self.config.name}] Stopping worker...")
        self.running = False
        
        if wait and self.worker_thread:
            self.worker_thread.join(timeout=self.config.shutdown_timeout)
            
        # wait for active tasks to complete
        self.executor.shutdown(wait=wait)
            
        logger.info(f"[{self.config.name}] Worker stopped")
        
    # main worker loop
    def _run(self):
        while self.running:
            try:
                # get a task from the queue
                task = self.queue.get(block=False)
                
                if task:
                    # submit task for processing
                    future = self.executor.submit(self._process_task, task)
                    with self._lock:
                        self.active_tasks[task.task_id] = future
                else:
                    # no task available, sleep briefly
                    time.sleep(self.config.poll_interval)
                    
            except Exception as e:
                logger.error(f"[{self.config.name}] Error in worker loop: {e}")
                time.sleep(self.config.poll_interval)
                
    def _process_task(self, task: Task):
        """
        process a single task
        
        Args:
            task: Task to process
        """
        logger.info(f"[{self.config.name}] Processing task {task.task_id}")
        
        try:
            # extract task type and data from payload
            if isinstance(task.payload, dict):
                task_type = task.payload.get('type', 'default')
                task_data = task.payload.get('data')
            else:
                task_type = 'default'
                task_data = task.payload
                
            # find appropriate handler
            handler = self.handlers.get(task_type)
            if not handler:
                raise ValueError(f"No handler registered for task type: {task_type}")
                
            # execute the handler
            start_time = time.time()
            result = handler(task_data)
            execution_time = time.time() - start_time
            
            # mark task as completed
            self.queue.mark_completed(task.task_id)
            logger.info(f"[{self.config.name}] Task {task.task_id} completed in {execution_time:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"[{self.config.name}] Task {task.task_id} failed: {e}")
            
            # call error handler if configured
            if self.config.error_handler:
                self.config.error_handler(task, e)
                
            # mark task as failed (will be retried or moved to DLQ)
            self.queue.mark_failed(task.task_id, str(e))
            
        finally:
            # remove from active tasks
            with self._lock:
                self.active_tasks.pop(task.task_id, None)
                
    # get worker statistics
    def get_stats(self) -> Dict[str, Any]:
        
        with self._lock:
            return {
                'name': self.config.name,
                'running': self.running,
                'active_tasks': len(self.active_tasks),
                'max_workers': self.config.max_workers,
                'handlers': list(self.handlers.keys())
            }


# worker pool
class WorkerPool:
    """
    manages multiple workers for load distribution
    """
    
    def __init__(self, queue: PriorityQueue):
        """
        initialize worker pool
        
        Args:
            queue: shared queue for all workers
        """
        self.queue = queue
        self.workers: Dict[str, Worker] = {}
        
    def add_worker(self, config: WorkerConfig) -> Worker:
        """
        add a new worker to the pool
        Args:
            config: worker configuration
        Returns:
            the created worker instance
        """
        worker = Worker(self.queue, config)
        self.workers[config.name] = worker
        return worker
        
    def start_all(self):
        """start all workers in the pool"""
        for worker in self.workers.values():
            worker.start()
            
    def stop_all(self, wait: bool = True):
        """
        stop all workers in the pool
        
        Args:
            wait: whether to wait for tasks to complete
        """
        for worker in self.workers.values():
            worker.stop(wait=wait)
            
    # get statistics for all workers        
    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_workers': len(self.workers),
            'workers': [worker.get_stats() for worker in self.workers.values()]
        }