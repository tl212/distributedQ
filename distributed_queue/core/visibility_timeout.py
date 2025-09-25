"""
Visibility timeout implementation for automatically rescue tasks
This module makes sure tasks don't get lost in limbo when workers crash mid-job.
When a worker grabs task, it temporarily disappears from view,
but if worker doesn't finish in time, the task pops back into the queue ready for 
someone else to try.
"""

import time
import threading
import logging
from typing import Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta

# prometheus visibility metrics helpers
try:
    from ..monitoring.metrics import (
        prom_set_active_leases,
        prom_inc_lease_expiration,
        prom_inc_lease_extension,
    )
except ImportError:
    # Fallback functions if prometheus helpers don't exist
    def prom_set_active_leases(count: int):
        pass
    def prom_inc_lease_expiration(by: int = 1):
        pass
    def prom_inc_lease_extension():
        pass

logger = logging.getLogger(__name__)


@dataclass
class TaskLease:
    # represents a lease on a task being processed

    task_id: str
    worker_id: str
    lease_time: float = field(default_factory=time.time)
    timeout: float = 300.0  # 5 minutes default

    def is_expired(self) -> bool:
        # check if this lease has expired
        return time.time() > (self.lease_time + self.timeout)

    def time_remaining(self) -> float:
        # get seconds remaining on lease
        remaining = (self.lease_time + self.timeout) - time.time()
        return max(0, remaining)


class VisibilityManager:
    """
    Manages task visibility timeouts for automatic recovery
    Features:
    - automatic task recovery on worker failure
    - configurable timeout periods
    - lease extension for long-running tasks
    - thread-safe operations
    """

    def __init__(self, default_timeout: float = 300.0):
        """
        Initialize visibility manager
        Args:
        - default_timeout: Default visibility timeout in seconds
        """
        self.default_timeout = default_timeout
        self.active_leases: Dict[str, TaskLease] = {}
        self._lock = threading.RLock()
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        self._recovery_callback = None

    def start_monitoring(self, check_interval: float = 30.0):
        """
        start background monitoring thread
        Args:
        - check_interval: How often to check for expired leases (seconds)
        """
        if self._running:
            logger.warning("Visibility monitoring already running")
            return

        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, args=(check_interval,), daemon=True
        )
        self._monitor_thread.start()
        logger.info(f"Started visibility monitoring (check every {check_interval}s)")

    def stop_monitoring(self):
        # stop background monitoring thread
        if not self._running:
            return

        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        logger.info("Stopped visibility monitoring")

    def acquire_lease(
        self, task_id: str, worker_id: str, timeout: Optional[float] = None
    ) -> TaskLease:
        """
        Acquire a lease on a task
        Args:
        - task_id: Task identifier
        - worker_id: Worker identifier
        - timeout: Custom timeout for this task (uses default if None)
        Returns: TaskLease object
        """
        with self._lock:
            if task_id in self.active_leases:
                existing = self.active_leases[task_id]
                if not existing.is_expired():
                    raise ValueError(
                        f"Task {task_id} already leased to {existing.worker_id}"
                    )
                else:
                    logger.warning(f"Overriding expired lease for task {task_id}")

            lease = TaskLease(
                task_id=task_id,
                worker_id=worker_id,
                timeout=timeout or self.default_timeout,
            )

            self.active_leases[task_id] = lease
            # update active leases gauge
            try:
                prom_set_active_leases(len(self.active_leases))
            except Exception:
                pass

            logger.debug(
                f"Acquired lease: task={task_id}, worker={worker_id}, timeout={lease.timeout}s"
            )

            return lease

    def release_lease(self, task_id: str, worker_id: str) -> bool:
        """
        Release a lease on a task (task completed/failed)
        Args:
        - task_id: Task identifier
        - worker_id: Worker identifier
        Returns: True if lease was released, False if not found or wrong worker
        """
        with self._lock:
            if task_id not in self.active_leases:
                logger.warning(f"No lease found for task {task_id}")
                return False

            lease = self.active_leases[task_id]
            if lease.worker_id != worker_id:
                logger.warning(
                    f"Worker {worker_id} cannot release lease owned by {lease.worker_id}"
                )
                return False

            del self.active_leases[task_id]
            # update active leases gauge
            try:
                prom_set_active_leases(len(self.active_leases))
            except Exception:
                pass

            logger.debug(f"Released lease: task={task_id}, worker={worker_id}")
            return True

    def extend_lease(
        self, task_id: str, worker_id: str, additional_time: float = None
    ) -> bool:
        """
        Extend lease for a long-running task
        Args:
        - task_id: Task identifier
        - worker_id: Worker identifier
        - additional_time: Additional seconds (uses default timeout if None)
        Returns:True if extended, False if not found or wrong worker
        """
        with self._lock:
            if task_id not in self.active_leases:
                logger.warning(f"No lease found for task {task_id}")
                return False

            lease = self.active_leases[task_id]
            if lease.worker_id != worker_id:
                logger.warning(
                    f"Worker {worker_id} cannot extend lease owned by {lease.worker_id}"
                )
                return False

            extension = additional_time or self.default_timeout
            lease.timeout += extension
            logger.info(f"Extended lease: task={task_id}, additional={extension}s")
            # increment lease extension counter
            try:
                prom_inc_lease_extension()
            except Exception:
                pass
            return True

    def check_lease(self, task_id: str) -> Optional[TaskLease]:
        """
        Check if a task has an active lease
        Args:
        - task_id: Task identifier
        Returns:TaskLease if active, None otherwise
        """
        with self._lock:
            lease = self.active_leases.get(task_id)
            if lease and not lease.is_expired():
                return lease
            return None

    def get_expired_leases(self) -> Set[str]:
        """
        Get all expired task IDs
        Returns:Set of task IDs with expired leases
        """
        with self._lock:
            expired = set()
            for task_id, lease in self.active_leases.items():
                if lease.is_expired():
                    expired.add(task_id)
                    logger.info(
                        f"Found expired lease: task={task_id}, worker={lease.worker_id}"
                    )
            return expired

    def set_recovery_callback(self, callback):
        """
        Set callback function for expired tasks
        Args:
        - callback: Function(task_id, worker_id) called when lease expires
        """
        self._recovery_callback = callback

    def _monitor_loop(self, check_interval: float):
        """
        Background thread that checks for expired leases
        Args:
            check_interval: How often to check (seconds)
        """
        logger.info("Visibility monitor thread started")

        while self._running:
            try:
                expired_tasks = self.get_expired_leases()

                if expired_tasks:
                    logger.info(f"Found {len(expired_tasks)} expired leases")

                    # increment expiration counter
                    try:
                        prom_inc_lease_expiration(len(expired_tasks))
                    except Exception:
                        pass

                    for task_id in expired_tasks:
                        with self._lock:
                            if task_id in self.active_leases:
                                lease = self.active_leases[task_id]

                                # call recovery callback if set
                                if self._recovery_callback:
                                    try:
                                        self._recovery_callback(
                                            task_id, lease.worker_id
                                        )
                                    except Exception as e:
                                        logger.error(f"Recovery callback failed: {e}")

                                # remove expired lease
                                del self.active_leases[task_id]
                                # update active leases gauge after removal
                                try:
                                    prom_set_active_leases(len(self.active_leases))
                                except Exception:
                                    pass
                                logger.info(f"Removed expired lease for task {task_id}")

                # sleep until next check
                time.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error in visibility monitor: {e}")
                time.sleep(check_interval)

        logger.info("Visibility monitor thread stopped")

    def get_stats(self) -> dict:
        # get visibility manager statistics"""
        with self._lock:
            active_count = len(self.active_leases)
            expired_count = len(self.get_expired_leases())

            lease_times = []
            for lease in self.active_leases.values():
                if not lease.is_expired():
                    lease_times.append(time.time() - lease.lease_time)

            return {
                "active_leases": active_count,
                "expired_leases": expired_count,
                "monitoring": self._running,
                "avg_lease_time": (
                    sum(lease_times) / len(lease_times) if lease_times else 0
                ),
                "max_lease_time": max(lease_times) if lease_times else 0,
                "default_timeout": self.default_timeout,
            }


# global instance
_visibility_manager = None


def get_visibility_manager() -> VisibilityManager:
    # get global visibility manager instance
    global _visibility_manager
    if _visibility_manager is None:
        _visibility_manager = VisibilityManager()
        _visibility_manager.start_monitoring()
    return _visibility_manager
