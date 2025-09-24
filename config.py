"""
configuration for distributed Q
"""

import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class QueueConfig:
    # queue configuration settings
    # backend selection
    use_redis: bool = False
    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "distqueue"
    
    # queue settings
    max_queue_size: int = 1000
    rate_limit: int = 100
    
    # worker settings
    num_workers: int = 2
    max_workers_per_pool: int = 5
    worker_poll_interval: float = 0.1
    
    # monitoring
    enable_monitoring: bool = True
    metrics_window_size: int = 300  # 5 minutes
    
    # task recovery (Redis only)
    enable_stale_recovery: bool = True
    stale_task_timeout: int = 300  # 5 minutes
    
    @classmethod
    def from_env(cls):
        # load configuration from environment variables
        return cls(
            use_redis=os.getenv("USE_REDIS", "false").lower() == "true",
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            redis_key_prefix=os.getenv("REDIS_KEY_PREFIX", "distqueue"),
            max_queue_size=int(os.getenv("MAX_QUEUE_SIZE", "1000")),
            rate_limit=int(os.getenv("RATE_LIMIT", "100")),
            num_workers=int(os.getenv("NUM_WORKERS", "2")),
            max_workers_per_pool=int(os.getenv("MAX_WORKERS_PER_POOL", "5")),
            worker_poll_interval=float(os.getenv("WORKER_POLL_INTERVAL", "0.1")),
            enable_monitoring=os.getenv("ENABLE_MONITORING", "true").lower() == "true",
            metrics_window_size=int(os.getenv("METRICS_WINDOW_SIZE", "300")),
            enable_stale_recovery=os.getenv("ENABLE_STALE_RECOVERY", "true").lower() == "true",
            stale_task_timeout=int(os.getenv("STALE_TASK_TIMEOUT", "300"))
        )
    
    def to_dict(self):
        # convert to dictionary
        return {
            "backend": "redis" if self.use_redis else "memory",
            "redis_url": self.redis_url if self.use_redis else None,
            "max_queue_size": self.max_queue_size,
            "rate_limit": self.rate_limit,
            "num_workers": self.num_workers,
            "monitoring_enabled": self.enable_monitoring
        }

# global config instance
_config = None

def get_config() -> QueueConfig:
    # get global configuration
    global _config
    if _config is None:
        _config = QueueConfig.from_env()
    return _config

def set_config(config: QueueConfig):
    # set global configuration
    global _config
    _config = config