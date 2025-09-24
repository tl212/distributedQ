"""
Metrics and monitoring for the distributed queue system

This module provides comprehensive metrics collection and monitoring
capabilities for queue operations, worker performance, and system health.
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import json

# prometheus client for exporting metrics
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

class MetricType(Enum):
    """Types of metrics we collect"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricPoint:
    """A single metric data point"""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary statistics for a metric"""
    count: int
    sum: float
    min: float
    max: float
    avg: float
    p50: float  # median
    p95: float
    p99: float


class RollingWindow:
    """Maintains a rolling window of metric values"""
    
    def __init__(self, window_size_seconds: int = 300):  # 5 minutes default
        self.window_size = window_size_seconds
        self.data: deque = deque()
        self._lock = threading.Lock()
    
    def add(self, value: float, timestamp: Optional[float] = None):
        """Add a value to the rolling window"""
        if timestamp is None:
            timestamp = time.time()
        
        with self._lock:
            self.data.append((timestamp, value))
            self._cleanup()
    
    def _cleanup(self):
        """Remove old data points outside the window"""
        cutoff = time.time() - self.window_size
        while self.data and self.data[0][0] < cutoff:
            self.data.popleft()
    
    def get_values(self) -> List[float]:
        """Get all values in the current window"""
        with self._lock:
            self._cleanup()
            return [value for _, value in self.data]
    
    def get_summary(self) -> Optional[MetricSummary]:
        """Calculate summary statistics"""
        values = self.get_values()
        if not values:
            return None
        
        values_sorted = sorted(values)
        count = len(values)
        
        return MetricSummary(
            count=count,
            sum=sum(values),
            min=min(values),
            max=max(values),
            avg=sum(values) / count,
            p50=values_sorted[count // 2],
            p95=values_sorted[int(count * 0.95)] if count > 1 else values_sorted[0],
            p99=values_sorted[int(count * 0.99)] if count > 1 else values_sorted[0]
        )


# prometheus metric definitions (default registry)
DQ_TASKS_SUBMITTED = Counter(
    "dq_tasks_submitted_total",
    "Total number of tasks submitted",
    ["type", "priority"]
)
DQ_TASKS_STARTED = Counter(
    "dq_tasks_started_total",
    "Total number of tasks started"
)
DQ_TASKS_COMPLETED = Counter(
    "dq_tasks_completed_total",
    "Total number of tasks completed",
    ["type"]
)
DQ_TASKS_FAILED = Counter(
    "dq_tasks_failed_total",
    "Total number of tasks failed",
    ["type"]
)
DQ_QUEUE_DEPTH = Gauge(
    "dq_queue_depth",
    "Current number of tasks in queue"
)
DQ_ACTIVE_WORKERS = Gauge(
    "dq_active_workers",
    "Current number of active workers"
)
DQ_ACTIVE_TASKS = Gauge(
    "dq_active_tasks",
    "Current number of active tasks being processed"
)
DQ_TASK_PROCESSING_SECONDS = Histogram(
    "dq_task_processing_seconds",
    "Task processing time in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]
)
DQ_TASK_WAIT_SECONDS = Histogram(
    "dq_task_wait_seconds",
    "Task wait time in seconds before processing",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]
)
DQ_ACTIVE_LEASES = Gauge(
    "dq_active_leases",
    "Current number of active task leases"
)
DQ_LEASE_EXPIRATIONS = Counter(
    "dq_lease_expirations_total",
    "Total number of lease expirations detected"
)
DQ_LEASE_EXTENSIONS = Counter(
    "dq_lease_extensions_total",
    "Total number of lease extensions performed"
)

class QueueMetrics:
    # comprehensive metrics collection for the queue system
    
    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.time()
        
        # counters
        self.tasks_submitted = 0
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.tasks_retried = 0
        self.tasks_dead_lettered = 0
        
        # gauges
        self.queue_depth = 0
        self.active_workers = 0
        self.active_tasks = 0
        
        # histograms (using rolling windows)
        self.task_processing_times = RollingWindow(300)  # 5 minutes
        self.task_wait_times = RollingWindow(300)
        self.queue_depths = RollingWindow(300)
        
        # task metrics by type
        self.tasks_by_type: Dict[str, int] = defaultdict(int)
        self.failures_by_type: Dict[str, int] = defaultdict(int)
        
        # rate calculations
        self.submission_rate_window = RollingWindow(60)  # 1 minute
        self.completion_rate_window = RollingWindow(60)
        
        # error tracking
        self.recent_errors: deque = deque(maxlen=100)
    
    def record_task_submitted(self, task_type: str = "default", priority: int = 0):
        """record a task submission"""
        with self._lock:
            self.tasks_submitted += 1
            self.tasks_by_type[task_type] += 1
            self.submission_rate_window.add(1)
            self.queue_depth += 1
            self.queue_depths.add(self.queue_depth)
            # prometheus updates
            try:
                DQ_TASKS_SUBMITTED.labels(type=task_type, priority=str(priority)).inc()
                DQ_QUEUE_DEPTH.set(self.queue_depth)
            except Exception:
                pass
    
    def record_task_completed(self, task_type: str = "default", 
                             processing_time: float = 0, 
                             wait_time: float = 0):
        """record successful task completion"""
        with self._lock:
            self.tasks_completed += 1
            self.completion_rate_window.add(1)
            self.queue_depth = max(0, self.queue_depth - 1)
            self.active_tasks = max(0, self.active_tasks - 1)
            
            if processing_time > 0:
                self.task_processing_times.add(processing_time)
            if wait_time > 0:
                self.task_wait_times.add(wait_time)
            # prometheus updates
            try:
                DQ_TASKS_COMPLETED.labels(type=task_type).inc()
                if processing_time > 0:
                    DQ_TASK_PROCESSING_SECONDS.observe(processing_time)
                if wait_time > 0:
                    DQ_TASK_WAIT_SECONDS.observe(wait_time)
                DQ_ACTIVE_TASKS.set(self.active_tasks)
                DQ_QUEUE_DEPTH.set(self.queue_depth)
            except Exception:
                pass
    
    def record_task_failed(self, task_type: str = "default", 
                          error: Optional[str] = None, 
                          will_retry: bool = False):
        """record task failure"""
        with self._lock:
            self.tasks_failed += 1
            self.failures_by_type[task_type] += 1
            
            if will_retry:
                self.tasks_retried += 1
            else:
                self.tasks_dead_lettered += 1
                self.queue_depth = max(0, self.queue_depth - 1)
            
            if error:
                self.recent_errors.append({
                    'timestamp': time.time(),
                    'task_type': task_type,
                    'error': error[:500]  # limit error message length
                })
            # prometheus updates
            try:
                DQ_TASKS_FAILED.labels(type=task_type).inc()
                DQ_QUEUE_DEPTH.set(self.queue_depth)
            except Exception:
                pass
    
    def record_task_started(self):
        """record task execution start"""
        with self._lock:
            self.active_tasks += 1
            # prometheus updates
            try:
                DQ_TASKS_STARTED.inc()
                DQ_ACTIVE_TASKS.set(self.active_tasks)
            except Exception:
                pass
    
    def update_worker_count(self, count: int):
        """update active worker count"""
        with self._lock:
            self.active_workers = count
            # prometheus updates
            try:
                DQ_ACTIVE_WORKERS.set(count)
            except Exception:
                pass
    
    def update_queue_depth(self, depth: int):
        """update current queue depth"""
        with self._lock:
            self.queue_depth = depth
            self.queue_depths.add(depth)
            # prometheus updates
            try:
                DQ_QUEUE_DEPTH.set(depth)
            except Exception:
                pass
    
    def get_uptime(self) -> float:
        """get system uptime in seconds"""
        return time.time() - self._start_time
    
    def get_throughput_rate(self) -> float:
        """calculate current throughput (tasks/second)"""
        summary = self.completion_rate_window.get_summary()
        if summary and summary.count > 0:
            window_size = min(60, self.get_uptime())  # use actual time if less than window
            return summary.sum / window_size
        return 0.0
    
    def get_submission_rate(self) -> float:
        """calculate current submission rate (tasks/second)"""
        summary = self.submission_rate_window.get_summary()
        if summary and summary.count > 0:
            window_size = min(60, self.get_uptime())
            return summary.sum / window_size
        return 0.0
    
    def get_success_rate(self) -> float:
        """calculate success rate percentage"""
        total = self.tasks_completed + self.tasks_failed
        if total == 0:
            return 100.0
        return (self.tasks_completed / total) * 100
    
    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """get a comprehensive snapshot of all metrics"""
        with self._lock:
            processing_summary = self.task_processing_times.get_summary()
            wait_summary = self.task_wait_times.get_summary()
            queue_depth_summary = self.queue_depths.get_summary()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'uptime_seconds': self.get_uptime(),
                'counters': {
                    'tasks_submitted': self.tasks_submitted,
                    'tasks_completed': self.tasks_completed,
                    'tasks_failed': self.tasks_failed,
                    'tasks_retried': self.tasks_retried,
                    'tasks_dead_lettered': self.tasks_dead_lettered
                },
                'gauges': {
                    'queue_depth': self.queue_depth,
                    'active_workers': self.active_workers,
                    'active_tasks': self.active_tasks
                },
                'rates': {
                    'submission_rate': self.get_submission_rate(),
                    'throughput_rate': self.get_throughput_rate(),
                    'success_rate': self.get_success_rate()
                },
                'processing_times': {
                    'count': processing_summary.count if processing_summary else 0,
                    'avg': processing_summary.avg if processing_summary else 0,
                    'min': processing_summary.min if processing_summary else 0,
                    'max': processing_summary.max if processing_summary else 0,
                    'p50': processing_summary.p50 if processing_summary else 0,
                    'p95': processing_summary.p95 if processing_summary else 0,
                    'p99': processing_summary.p99 if processing_summary else 0
                } if processing_summary else None,
                'wait_times': {
                    'count': wait_summary.count if wait_summary else 0,
                    'avg': wait_summary.avg if wait_summary else 0,
                    'min': wait_summary.min if wait_summary else 0,
                    'max': wait_summary.max if wait_summary else 0,
                    'p50': wait_summary.p50 if wait_summary else 0,
                    'p95': wait_summary.p95 if wait_summary else 0,
                    'p99': wait_summary.p99 if wait_summary else 0
                } if wait_summary else None,
                'queue_depth_stats': {
                    'current': self.queue_depth,
                    'avg': queue_depth_summary.avg if queue_depth_summary else 0,
                    'min': queue_depth_summary.min if queue_depth_summary else 0,
                    'max': queue_depth_summary.max if queue_depth_summary else 0
                } if queue_depth_summary else None,
                'tasks_by_type': dict(self.tasks_by_type),
                'failures_by_type': dict(self.failures_by_type),
                'recent_errors': list(self.recent_errors)[-10:]  # Last 10 errors
            }
    
    def get_health_status(self) -> Dict[str, Any]:
        """get system health status"""
        metrics = self.get_metrics_snapshot()
        
        # define health thresholds
        is_healthy = True
        warnings = []
        
        # check queue depth
        if self.queue_depth > 1000:
            warnings.append(f"High queue depth: {self.queue_depth}")
            is_healthy = False
        
        # check success rate
        success_rate = self.get_success_rate()
        if success_rate < 95:
            warnings.append(f"Low success rate: {success_rate:.1f}%")
            is_healthy = False
        
        # check if workers are active
        if self.active_workers == 0 and self.queue_depth > 0:
            warnings.append("No active workers with pending tasks")
            is_healthy = False
        
        # check processing times
        processing_summary = self.task_processing_times.get_summary()
        if processing_summary and processing_summary.p95 > 10:  # 10 seconds
            warnings.append(f"High p95 processing time: {processing_summary.p95:.2f}s")
        
        return {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'uptime_seconds': self.get_uptime(),
            'checks': {
                'queue_depth': 'ok' if self.queue_depth < 1000 else 'warning',
                'success_rate': 'ok' if success_rate >= 95 else 'warning',
                'workers': 'ok' if self.active_workers > 0 or self.queue_depth == 0 else 'critical',
                'processing_time': 'ok' if not processing_summary or processing_summary.p95 < 10 else 'warning'
            },
            'warnings': warnings,
            'metrics_summary': {
                'queue_depth': self.queue_depth,
                'active_workers': self.active_workers,
                'success_rate': f"{success_rate:.1f}%",
                'throughput': f"{self.get_throughput_rate():.2f} tasks/sec"
            }
        }
    
    def export_prometheus(self) -> str:
        """export metrics in Prometheus format"""
        lines = []
        
        # add metric descriptions
        lines.append("# HELP tasks_submitted_total Total number of tasks submitted")
        lines.append("# TYPE tasks_submitted_total counter")
        lines.append(f"tasks_submitted_total {self.tasks_submitted}")
        
        lines.append("# HELP tasks_completed_total Total number of tasks completed")
        lines.append("# TYPE tasks_completed_total counter")
        lines.append(f"tasks_completed_total {self.tasks_completed}")
        
        lines.append("# HELP tasks_failed_total Total number of tasks failed")
        lines.append("# TYPE tasks_failed_total counter")
        lines.append(f"tasks_failed_total {self.tasks_failed}")
        
        lines.append("# HELP queue_depth Current number of tasks in queue")
        lines.append("# TYPE queue_depth gauge")
        lines.append(f"queue_depth {self.queue_depth}")
        
        lines.append("# HELP active_workers Number of active workers")
        lines.append("# TYPE active_workers gauge")
        lines.append(f"active_workers {self.active_workers}")
        
        lines.append("# HELP throughput_rate Current throughput in tasks per second")
        lines.append("# TYPE throughput_rate gauge")
        lines.append(f"throughput_rate {self.get_throughput_rate():.4f}")
        
        # processing time percentiles
        processing_summary = self.task_processing_times.get_summary()
        if processing_summary:
            lines.append("# HELP task_processing_seconds Task processing time in seconds")
            lines.append("# TYPE task_processing_seconds summary")
            lines.append(f'task_processing_seconds{{quantile="0.5"}} {processing_summary.p50:.4f}')
            lines.append(f'task_processing_seconds{{quantile="0.95"}} {processing_summary.p95:.4f}')
            lines.append(f'task_processing_seconds{{quantile="0.99"}} {processing_summary.p99:.4f}')
            lines.append(f"task_processing_seconds_sum {processing_summary.sum:.4f}")
            lines.append(f"task_processing_seconds_count {processing_summary.count}")
        
        return '\n'.join(lines)


# global metrics instance
_metrics = None


def get_metrics() -> QueueMetrics:
    """get the global metrics instance"""
    global _metrics
    if _metrics is None:
        _metrics = QueueMetrics()
    return _metrics


def reset_metrics():
    """reset all metrics (useful for testing)"""
    global _metrics
    _metrics = QueueMetrics()


# convenience helpers for visibility metrics integration
def prom_set_active_leases(count: int):
    try:
        DQ_ACTIVE_LEASES.set(count)
    except Exception:
        pass


def prom_inc_lease_expiration(by: int = 1):
    try:
        DQ_LEASE_EXPIRATIONS.inc(by)
    except Exception:
        pass


def prom_inc_lease_extension():
    try:
        DQ_LEASE_EXTENSIONS.inc()
    except Exception:
        pass
