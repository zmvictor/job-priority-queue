import pytest
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobStatus, JobCreate
from app.models.queue import PriorityQueue

def create_test_job(priority: int, submitted_delta: timedelta = timedelta()) -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        id=f"test-{priority}",
        name=f"test-job-{priority}",
        priority=priority,
        submitted_at=now - submitted_delta,
        status=JobStatus.SUBMITTED,
        metadata={},
        last_status_change=now,
    )

class TestPriorityQueue:
    def test_basic_priority_ordering(self):
        queue = PriorityQueue()
        # Add jobs in reverse priority order
        jobs = [create_test_job(i) for i in range(3)]
        for job in jobs:
            queue.enqueue(job)
        
        # Should dequeue in priority order (highest first)
        job = queue.dequeue()
        assert job is not None and job.priority == 2
        job = queue.dequeue()
        assert job is not None and job.priority == 1
        job = queue.dequeue()
        assert job is not None and job.priority == 0
        assert queue.dequeue() is None

    def test_wait_time_priority_boost(self):
        queue = PriorityQueue()
        # Create two jobs with same priority but different wait times
        recent_job = create_test_job(50)
        old_job = create_test_job(50, submitted_delta=timedelta(hours=12))
        
        queue.enqueue(recent_job)
        queue.enqueue(old_job)
        
        # Old job should come out first due to wait time boost
        dequeued = queue.dequeue()
        assert dequeued is not None and dequeued.id == old_job.id
        
        # Verify the priority calculation
        old_priority = queue._calculate_priority(old_job)
        recent_priority = queue._calculate_priority(recent_job)
        # For same priority, compare wait times (second tuple element)
        assert old_priority[0] == recent_priority[0]  # Same base priority
        assert old_priority[1] < recent_priority[1]  # Older job has more negative wait time

    def test_thread_safety(self):
        import threading
        queue = PriorityQueue()
        jobs = [create_test_job(i) for i in range(100)]
        
        def enqueue_batch(batch):
            for job in batch:
                queue.enqueue(job)
        
        # Create multiple threads to enqueue jobs
        threads = []
        batch_size = 20
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i+batch_size]
            thread = threading.Thread(target=enqueue_batch, args=(batch,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all jobs were enqueued
        assert queue.size == len(jobs)
        
        # Verify dequeue order maintains priority
        prev_priority = None
        while True:
            job = queue.dequeue()
            if not job:
                break
            effective_priority = queue._calculate_priority(job)
            if prev_priority is not None:
                # For tuples, the comparison should work naturally
                # (-priority, -wait_time) means higher priority and longer wait come first
                assert effective_priority >= prev_priority
            prev_priority = effective_priority

    def test_remove_job(self):
        queue = PriorityQueue()
        job = create_test_job(50)
        queue.enqueue(job)
        
        # Remove should return and remove the job
        removed = queue.remove(job.id)
        assert removed is not None and removed.id == job.id
        assert queue.get_job(job.id) is None
        
        # Remove non-existent job should return None
        assert queue.remove("non-existent") is None

    def test_update_priority(self):
        queue = PriorityQueue()
        job = create_test_job(50)
        queue.enqueue(job)
        
        # Record initial priority
        initial_priority = queue._calculate_priority(job)
        
        # Simulate time passing
        job.submitted_at -= timedelta(hours=5)
        
        # Update priority
        assert queue.update_priority(job.id)
        
        # Get job and verify priority increased
        updated_job = queue.get_job(job.id)
        updated_priority = queue._calculate_priority(updated_job)
        # Same base priority but more negative wait time
        assert updated_priority[0] == initial_priority[0]  # Same base priority
        assert updated_priority[1] < initial_priority[1]  # More negative wait time after update

    def test_peek_without_removal(self):
        queue = PriorityQueue()
        job = create_test_job(50)
        queue.enqueue(job)
        
        # Peek should return job without removing
        peeked = queue.peek()
        assert peeked is not None and peeked.id == job.id
        assert queue.size == 1
        
        # Peek again should return same job
        peeked = queue.peek()
        assert peeked is not None and peeked.id == job.id
