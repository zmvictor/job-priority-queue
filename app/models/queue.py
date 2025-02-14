from threading import Lock
from typing import Optional, List, Dict, Tuple, TypeAlias
import heapq
from datetime import datetime, timezone
from .job import Job, JobStatus

# Type alias for priority tuple (negative_priority, negative_wait_time, submission_time)
PriorityTuple: TypeAlias = tuple[float, float, float]

class PriorityQueue:
    def __init__(self):
        self._queue: List[Tuple[PriorityTuple, str]] = []  # (priority_tuple, job_id)
        self._lock = Lock()
        self._job_map: Dict[str, Job] = {}  # For O(1) job lookups
        
    def enqueue(self, job: Job) -> None:
        """Thread-safe enqueue operation."""
        with self._lock:
            effective_priority = self._calculate_priority(job)
            heapq.heappush(self._queue, (effective_priority, job.id))
            self._job_map[job.id] = job
            
    def dequeue(self) -> Optional[Job]:
        """Thread-safe dequeue operation returning highest priority job."""
        with self._lock:
            while self._queue:
                _, job_id = heapq.heappop(self._queue)
                if job_id in self._job_map:
                    return self._job_map.pop(job_id)
            return None
    
    def peek(self) -> Optional[Job]:
        """Thread-safe peek operation."""
        with self._lock:
            while self._queue:
                _, job_id = self._queue[0]
                if job_id in self._job_map:
                    return self._job_map[job_id]
                heapq.heappop(self._queue)  # Remove stale entry
            return None
    
    def remove(self, job_id: str) -> Optional[Job]:
        """Thread-safe removal of specific job."""
        with self._lock:
            if job_id in self._job_map:
                job = self._job_map.pop(job_id)
                # Note: We lazily clean up the heap when dequeuing/peeking
                return job
            return None
            
    def get_job(self, job_id: str) -> Optional[Job]:
        """Thread-safe job lookup."""
        with self._lock:
            return self._job_map.get(job_id)
    
    def update_priority(self, job_id: str) -> bool:
        """Thread-safe priority update based on current wait time."""
        with self._lock:
            if job_id not in self._job_map:
                return False
            job = self._job_map[job_id]
            effective_priority = self._calculate_priority(job)
            heapq.heappush(self._queue, (effective_priority, job_id))
            return True
    
    def _calculate_priority(self, job: Job) -> PriorityTuple:
        """Calculate effective priority including wait time factor.
        
        Priority is calculated as a tuple: (-priority, -wait_time_hours, submission_time)
        All elements are ordered for proper priority queue behavior.
        """
        job.update_wait_time_weight()  # Update the weight based on current wait time
        
        # For jobs with same priority, prioritize wait time weight
        return (
            float(-job.priority),  # Base priority first
            float(-job.wait_time_weight),  # Wait time weight second (higher = more negative)
            float(job.submitted_at.timestamp())  # Finally by submission time
        )
    
    @property
    def size(self) -> int:
        """Thread-safe queue size."""
        with self._lock:
            return len(self._job_map)
    
    def clear(self) -> None:
        """Thread-safe queue clear operation."""
        with self._lock:
            self._queue.clear()
            self._job_map.clear()
