"""
main FastAPI application for Distributed Queue
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os

from ..core.hybrid_queue import HybridQueue, StorageBackend
from ..core.worker import WorkerPool, WorkerConfig
from .routes import QueueRouter

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# global instances
queue = None
worker_pool = None
queue_router = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # manage application lifecycle
    global queue, worker_pool, queue_router
    
    # startup
    logger.info("Starting Distributed Queue API...")
    
    # get configuration from enviroment variables 
    use_redis = os.getenv("USE_REDIS", "false").lower() == "true"
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    max_queue_size = int(os.getenv("MAX_QUEUE_SIZE", "1000"))
    rate_limit = int(os.getenv("RATE_LIMIT", "100"))

    # initialize hybrid queue with Redis or in-memory backend
    backend = StorageBackend.REDIS if use_redis else StorageBackend.MEMORY

    queue = HybridQueue(
        backend=backend,
        redis_url=redis_url,
        max_size=max_queue_size,
        rate_limit=rate_limit,
        key_prefix="distqueue"
        )

    # log which backend is being used
    queue_health = queue.health_check()
    logger.info(f"Queue backend: {queue_health['backend']}")
    logger.info(f"Queue status: {queue_health['message']}")

    # initialize worker pool
    worker_pool = WorkerPool(queue)
    
    # add default workers
    for i in range(2):
        config = WorkerConfig(
            name=f"default-worker-{i}",
            max_workers=3,
            poll_interval=0.1
        )
        worker = worker_pool.add_worker(config)
        
        # register default handlers
        def simple_handler(data):
            import time
            import random
            time.sleep(random.uniform(0.1, 0.5))
            return f"Processed: {data}"
        
        def math_handler(data):
            operation = data.get("operation")
            a = data.get("a", 0)
            b = data.get("b", 0)
            
            if operation == "add":
                return a + b
            elif operation == "multiply":
                return a * b
            elif operation == "divide":
                if b == 0:
                    raise ValueError("Division by zero")
                return a / b
            else:
                return f"Unknown operation: {operation}"
        
        worker.register_handler("default", simple_handler)
        worker.register_handler("simple", simple_handler)
        worker.register_handler("math", math_handler)
    
    # start workers
    worker_pool.start_all()
    logger.info(f"Started {len(worker_pool.workers)} default workers")
    
    # initialize router
    queue_router = QueueRouter(queue, worker_pool)
    app.include_router(queue_router.router)
    
    yield
    
    # shutdown
    logger.info("Shutting down Distributed Queue API...")
    worker_pool.stop_all(wait=True)
    logger.info("All workers stopped")


# create FastAPI app
app = FastAPI(
    title="Distributed Queue API",
    description="A distributed task queue system with priority support",
    version="2.0.0",
    lifespan=lifespan
)

# configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    # root endpoint
    return {
        "name": "Distributed Queue API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health"
    }


@app.get("/info")
async def info():
    # get system information
    global queue, worker_pool
    
    if not queue or not worker_pool:
        return {"error": "System not initialized"}
    
    worker_stats = worker_pool.get_stats()
    
    return {
        "queue": {
            "size": queue.size(),
            "max_size": queue._max_size,
            "rate_limit": queue._rate_limit,
            "dead_letters": len(queue.get_dead_letters())
        },
        "workers": {
            "total": worker_stats["total_workers"],
            "running": sum(1 for w in worker_stats["workers"] if w["running"]),
            "details": worker_stats["workers"]
        }
    }


if __name__ == "__main__":
    # run the app
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
    
