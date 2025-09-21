"""
unit tests for Worker implementation
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch
from distributed_queue.core.queue import PriorityQueue, Task
from distributed_queue.core.worker import Worker, WorkerConfig, WorkerPool

# test suite for Worker class
class TestWorker:
    
    def test_worker_initialization(self):
        queue = PriorityQueue()
        config = WorkerConfig(name="test-worker", max_workers=3)
        worker = Worker(queue, config)
        
        assert worker.config.name == "test-worker"
        assert worker.config.max_workers == 3
        assert not worker.running
        assert len(worker.handlers) == 0
    
    # test registering task handlers
    def test_handler_registration(self):
        queue = PriorityQueue()
        config = WorkerConfig(name="test-worker")
        worker = Worker(queue, config)
        
        def handler1(data):
            return f"Handler1: {data}"
        
        def handler2(data):
            return f"Handler2: {data}"
        
        worker.register_handler("type1", handler1)
        worker.register_handler("type2", handler2)
        
        assert len(worker.handlers) == 2
        assert worker.handlers["type1"] == handler1
        assert worker.handlers["type2"] == handler2
    
    def test_worker_start_stop(self):
        # test starting and stopping worker
        queue = PriorityQueue()
        config = WorkerConfig(name="test-worker")
        worker = Worker(queue, config)
        
        # register a dummy handler
        worker.register_handler("test", lambda x: x)
        
        # start worker
        worker.start()
        assert worker.running
        assert worker.worker_thread is not None
        assert worker.worker_thread.is_alive()
        
        # stop worker
        worker.stop(wait=True)
        assert not worker.running
        assert not worker.worker_thread.is_alive()
    
    def test_task_processing(self):
        # test processing tasks with worker
        queue = PriorityQueue()
        config = WorkerConfig(name="test-worker", poll_interval=0.01)
        worker = Worker(queue, config)
        
        results = []
        
        def test_handler(data):
            results.append(data)
            return f"Processed: {data}"
        
        worker.register_handler("test", test_handler)
        
        # add tasks to queue
        for i in range(3):
            task = Task(priority=i, payload={"type": "test", "data": f"Task-{i}"})
            queue.put(task)
        
        # start worker
        worker.start()
        
        # wait for processing
        timeout = time.time() + 5
        while len(results) < 3 and time.time() < timeout:
            time.sleep(0.1)
        
        # stop worker
        worker.stop(wait=True)
        
        # check results
        assert len(results) == 3
        assert "Task-0" in results
        assert "Task-1" in results
        assert "Task-2" in results
    
    def test_error_handling(self):
        # test error handling in worker
        queue = PriorityQueue()
        
        error_log = []
        
        def error_handler(task, error):
            error_log.append((task.task_id, str(error)))
        
        config = WorkerConfig(
            name="test-worker",
            poll_interval=0.01,
            error_handler=error_handler
        )
        worker = Worker(queue, config)
        
        def failing_handler(data):
            raise ValueError(f"Failed processing: {data}")
        
        worker.register_handler("failing", failing_handler)
        
        # add task
        task = Task(priority=1, payload={"type": "failing", "data": "test"})
        task_id = task.task_id
        queue.put(task)
        
        # start worker
        worker.start()
        
        # wait for processing
        timeout = time.time() + 2
        while len(error_log) < 1 and time.time() < timeout:
            time.sleep(0.1)
        
        # stop worker
        worker.stop(wait=True)
        
        # check error was handled
        assert len(error_log) == 1
        assert error_log[0][0] == task_id
        assert "Failed processing" in error_log[0][1]
    
    def test_worker_stats(self):
        # test worker statistics
        queue = PriorityQueue()
        config = WorkerConfig(name="test-worker", max_workers=5)
        worker = Worker(queue, config)
        
        worker.register_handler("test", lambda x: x)
        
        stats = worker.get_stats()
        assert stats["name"] == "test-worker"
        assert stats["running"] == False
        assert stats["active_tasks"] == 0
        assert stats["max_workers"] == 5
        assert stats["handlers"] == ["test"]
        
        # start worker
        worker.start()
        stats = worker.get_stats()
        assert stats["running"] == True
        
        worker.stop(wait=True)
    
    def test_concurrent_task_processing(self):
        # test concurrent task processing with thread pool
        queue = PriorityQueue()
        config = WorkerConfig(
            name="test-worker",
            max_workers=3,
            poll_interval=0.01
        )
        worker = Worker(queue, config)
        
        processing_times = []
        lock = threading.Lock()
        
        def slow_handler(data):
            start = time.time()
            time.sleep(0.5)  # simulate slow processing
            with lock:
                processing_times.append((data, time.time() - start))
            return f"Done: {data}"
        
        worker.register_handler("slow", slow_handler)
        
        # add multiple tasks
        for i in range(3):
            task = Task(priority=i, payload={"type": "slow", "data": f"Task-{i}"})
            queue.put(task)
        
        # start worker
        start_time = time.time()
        worker.start()
        
        # wait for all tasks to complete
        timeout = time.time() + 5
        while len(processing_times) < 3 and time.time() < timeout:
            time.sleep(0.1)
        
        total_time = time.time() - start_time
        
        # stop worker
        worker.stop(wait=True)
        
        # with max_workers=3, all tasks should process in parallel
        # total time should be around 0.5 seconds, not 1.5 seconds
        assert len(processing_times) == 3
        assert total_time < 1.0  # should be parallel, not sequential


class TestWorkerPool:
    # test suite for WorkerPool class
    
    def test_worker_pool_initialization(self):
        # test worker pool initialization
        queue = PriorityQueue()
        pool = WorkerPool(queue)
        
        assert pool.queue == queue
        assert len(pool.workers) == 0
    
    def test_add_worker_to_pool(self):
        # test adding workers to pool
        queue = PriorityQueue()
        pool = WorkerPool(queue)
        
        config1 = WorkerConfig(name="worker-1")
        config2 = WorkerConfig(name="worker-2")
        
        worker1 = pool.add_worker(config1)
        worker2 = pool.add_worker(config2)
        
        assert len(pool.workers) == 2
        assert pool.workers["worker-1"] == worker1
        assert pool.workers["worker-2"] == worker2
    
    def test_pool_start_stop(self):
        # test starting and stopping all workers in pool
        queue = PriorityQueue()
        pool = WorkerPool(queue)
        
        # add workers
        for i in range(3):
            config = WorkerConfig(name=f"worker-{i}")
            worker = pool.add_worker(config)
            worker.register_handler("test", lambda x: x)
        
        # start all
        pool.start_all()
        for worker in pool.workers.values():
            assert worker.running
        
        # stop all
        pool.stop_all(wait=True)
        for worker in pool.workers.values():
            assert not worker.running
    
    def test_pool_load_distribution(self):
        # test load distribution across workers in pool
        queue = PriorityQueue()
        pool = WorkerPool(queue)
        
        results = {}
        lock = threading.Lock()
        
        def create_handler(worker_name):
            def handler(data):
                with lock:
                    if worker_name not in results:
                        results[worker_name] = []
                    results[worker_name].append(data)
                time.sleep(0.1)
                return f"{worker_name}: {data}"
            return handler
        
        # add multiple workers
        for i in range(2):
            config = WorkerConfig(
                name=f"worker-{i}",
                max_workers=2,
                poll_interval=0.01
            )
            worker = pool.add_worker(config)
            worker.register_handler("test", create_handler(f"worker-{i}"))
        
        # add tasks
        for i in range(6):
            task = Task(priority=i, payload={"type": "test", "data": f"Task-{i}"})
            queue.put(task)
        
        # start pool
        pool.start_all()
        
        # wait for processing
        timeout = time.time() + 5
        total_processed = 0
        while total_processed < 6 and time.time() < timeout:
            with lock:
                total_processed = sum(len(v) for v in results.values())
            time.sleep(0.1)
        
        # stop pool
        pool.stop_all(wait=True)
        
        # check that both workers processed some tasks
        assert len(results) == 2
        assert all(len(tasks) > 0 for tasks in results.values())
        total = sum(len(v) for v in results.values())
        assert total == 6
    
    def test_pool_stats(self):
        # test worker pool statistics
        queue = PriorityQueue()
        pool = WorkerPool(queue)
        
        # add workers
        for i in range(2):
            config = WorkerConfig(name=f"worker-{i}", max_workers=3)
            pool.add_worker(config)
        
        stats = pool.get_stats()
        assert stats["total_workers"] == 2
        assert len(stats["workers"]) == 2
        assert all(w["name"] in ["worker-0", "worker-1"] for w in stats["workers"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])