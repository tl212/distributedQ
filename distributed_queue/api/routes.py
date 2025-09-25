"""
fastAPI routes for queue operations
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Depends
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid

from ..core.queue import Task, TaskStatus
from ..core.worker import WorkerConfig
from ..monitoring.metrics import get_metrics
from .security import require_api_key


# pydantic models for request/response
class TaskSubmission(BaseModel):
    # model for submitting a new task
    priority: int = Field(ge=0, le=100, description="Task priority (0=highest, 100=lowest)")
    payload: Dict[str, Any] = Field(description="Task payload data")
    execute_after: Optional[datetime] = Field(None, description="Delayed execution time")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")


class TaskResponse(BaseModel):
    # model for task response
    task_id: str
    priority: int
    status: str
    created_at: float
    execute_after: Optional[float]
    retry_count: int
    max_retries: int
    payload: Dict[str, Any]


class QueueStats(BaseModel):
    # model for queue statistics
    queue_size: int
    dead_letter_count: int
    max_size: Optional[int]
    rate_limit: Optional[int]
    workers: List[Dict[str, Any]]


class WorkerConfigRequest(BaseModel):
    # model for creating a new worker
    name: str
    max_workers: int = Field(default=5, ge=1, le=20)
    poll_interval: float = Field(default=0.1, ge=0.01, le=1.0)


class MessageResponse(BaseModel):
    # generic message response
    message: str
    details: Optional[Dict[str, Any]] = None


class QueueRouter:
    # fastAPI router for queue operations
    
    def __init__(self, queue, worker_pool):
        """
        initialize router with queue and worker pool
        
        Args:
            queue: PriorityQueue instance
            worker_pool: WorkerPool instance
        """
        self.queue = queue
        self.worker_pool = worker_pool
        self.router = APIRouter(prefix="/api/v1", tags=["queue"])
        self.task_store = {}  # simple in-memory store for task details
        self._setup_routes()
    
    def _setup_routes(self):
        # setup all API routes
        
        @self.router.post("/tasks", response_model=TaskResponse, dependencies=[Depends(require_api_key)])
        async def submit_task(task_submission: TaskSubmission):
            # submit a new task to the queue
            try:
                # Create task
                task = Task(
                    priority=task_submission.priority,
                    payload=task_submission.payload,
                    execute_after=task_submission.execute_after.timestamp() if task_submission.execute_after else None,
                    max_retries=task_submission.max_retries
                )
                
                # add to queue
                success = self.queue.put(task, block=False)
                if not success:
                    raise HTTPException(status_code=503, detail="Queue is full")
                
                # store task details
                self.task_store[task.task_id] = task
                
                return TaskResponse(
                    task_id=task.task_id,
                    priority=task.priority,
                    status=task.status.value,
                    created_at=task.created_at,
                    execute_after=task.execute_after,
                    retry_count=task.retry_count,
                    max_retries=task.max_retries,
                    payload=task.payload
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/tasks/{task_id}", response_model=TaskResponse)
        async def get_task(task_id: str):
            # get task details by ID
            if task_id not in self.task_store:
                # check if it's in the dead letter queue
                dead_letters = self.queue.get_dead_letters()
                for task in dead_letters:
                    if task.task_id == task_id:
                        return TaskResponse(
                            task_id=task.task_id,
                            priority=task.priority,
                            status=task.status.value,
                            created_at=task.created_at,
                            execute_after=task.execute_after,
                            retry_count=task.retry_count,
                            max_retries=task.max_retries,
                            payload=task.payload
                        )
                raise HTTPException(status_code=404, detail="Task not found")
            
            task = self.task_store[task_id]
            return TaskResponse(
                task_id=task.task_id,
                priority=task.priority,
                status=task.status.value,
                created_at=task.created_at,
                execute_after=task.execute_after,
                retry_count=task.retry_count,
                max_retries=task.max_retries,
                payload=task.payload
            )
        
        @self.router.get("/stats", response_model=QueueStats)
        async def get_queue_stats():
            # get queue statistics
            worker_stats = self.worker_pool.get_stats()
            
            return QueueStats(
                queue_size=self.queue.size(),
                dead_letter_count=len(self.queue.get_dead_letters()),
                max_size=self.queue._max_size,
                rate_limit=self.queue._rate_limit,
                workers=worker_stats.get("workers", [])
            )
        
        @self.router.get("/dead-letters", response_model=List[TaskResponse])
        async def get_dead_letters(
            limit: int = Query(default=100, ge=1, le=1000),
            offset: int = Query(default=0, ge=0)
        ):
            # get tasks in dead letter queue
            dead_letters = self.queue.get_dead_letters()
            
            # apply pagination
            paginated = dead_letters[offset:offset + limit]
            
            return [
                TaskResponse(
                    task_id=task.task_id,
                    priority=task.priority,
                    status=task.status.value,
                    created_at=task.created_at,
                    execute_after=task.execute_after,
                    retry_count=task.retry_count,
                    max_retries=task.max_retries,
                    payload=task.payload
                )
                for task in paginated
            ]
        
        @self.router.post("/workers", response_model=MessageResponse, dependencies=[Depends(require_api_key)])
        async def create_worker(worker_config: WorkerConfigRequest):
            # create a new worker
            try:
                config = WorkerConfig(
                    name=worker_config.name,
                    max_workers=worker_config.max_workers,
                    poll_interval=worker_config.poll_interval
                )
                
                # check if worker with this name already exists
                if worker_config.name in self.worker_pool.workers:
                    raise HTTPException(status_code=400, detail="Worker with this name already exists")
                
                worker = self.worker_pool.add_worker(config)
                
                # register default handlers (you'd customize this based on your needs)
                # For demo purposes, we'll just register a simple handler
                def default_handler(data):
                    return f"Processed: {data}"
                
                worker.register_handler("default", default_handler)
                
                return MessageResponse(
                    message=f"Worker '{worker_config.name}' created successfully",
                    details={"worker_name": worker_config.name, "max_workers": worker_config.max_workers}
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/workers/start", response_model=MessageResponse, dependencies=[Depends(require_api_key)])
        async def start_workers():
            # start all workers
            try:
                self.worker_pool.start_all()
                stats = self.worker_pool.get_stats()
                return MessageResponse(
                    message="All workers started",
                    details=stats
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/workers/stop", response_model=MessageResponse, dependencies=[Depends(require_api_key)])
        async def stop_workers():
            # stop all workers
            try:
                self.worker_pool.stop_all(wait=True)
                return MessageResponse(
                    message="All workers stopped",
                    details={"stopped_workers": len(self.worker_pool.workers)}
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.delete("/workers/{worker_name}", response_model=MessageResponse, dependencies=[Depends(require_api_key)])
        async def remove_worker(worker_name: str):
            # remove a specific worker
            if worker_name not in self.worker_pool.workers:
                raise HTTPException(status_code=404, detail="Worker not found")
            
            try:
                worker = self.worker_pool.workers[worker_name]
                worker.stop(wait=True)
                del self.worker_pool.workers[worker_name]
                
                return MessageResponse(
                    message=f"Worker '{worker_name}' removed successfully"
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.delete("/queue/clear", response_model=MessageResponse, dependencies=[Depends(require_api_key)])
        async def clear_queue():
            # clear all tasks from queue
            try:
                prev_size = self.queue.size()
                prev_dlq = len(self.queue.get_dead_letters())
                
                self.queue.clear()
                self.task_store.clear()
                
                return MessageResponse(
                    message="Queue cleared successfully",
                    details={
                        "tasks_cleared": prev_size,
                        "dead_letters_cleared": prev_dlq
                    }
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/health")
        async def health_check():
            """health check endpoint"""
            metrics = get_metrics()
            health_status = metrics.get_health_status()
            return health_status
        
        @self.router.get("/metrics/json")
        async def get_metrics_json():
            """get detailed metrics in JSON format"""
            metrics = get_metrics()
            return metrics.get_metrics_snapshot()
        
        @self.router.get("/metrics")
        async def get_prometheus_metrics():
            """Expose Prometheus metrics for scraping"""
            from fastapi.responses import Response
            from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
            data = generate_latest()
            return Response(content=data, media_type=CONTENT_TYPE_LATEST)
