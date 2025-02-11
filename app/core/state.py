from threading import Lock
from typing import Dict, Optional, List
from datetime import datetime
import json
import os
from app.models.job import Job, JobStatus, JobJSONEncoder

class JobStateManager:
    def __init__(self, state_file: str = "job_state.json"):
        self._lock = Lock()
        self._running_jobs: Dict[str, Job] = {}
        self._state_file = state_file
        self._load_state()
        
    def clear(self) -> None:
        """Clear all running jobs and state."""
        with self._lock:
            self._running_jobs.clear()
            if os.path.exists(self._state_file):
                os.remove(self._state_file)
    
    def transition_to_running(self, job: Job) -> None:
        """Thread-safe transition to running state."""
        with self._lock:
            job.update_status(JobStatus.RUNNING)
            self._running_jobs[job.id] = job
            self._save_state()
    
    def preempt_job(self, job: Job) -> None:
        """Thread-safe job preemption."""
        with self._lock:
            job.increment_preemption()
            self._running_jobs.pop(job.id, None)
            self._save_state()
    
    def complete_job(self, job: Job, success: bool = True) -> None:
        """Thread-safe job completion."""
        with self._lock:
            job.update_status(JobStatus.COMPLETED if success else JobStatus.FAILED)
            self._running_jobs.pop(job.id, None)
            self._save_state()
    
    def get_running_jobs(self) -> List[Job]:
        """Thread-safe access to running jobs."""
        with self._lock:
            return list(self._running_jobs.values())
    
    def get_job_state(self, job_id: str) -> Optional[Job]:
        """Thread-safe job state lookup."""
        with self._lock:
            return self._running_jobs.get(job_id)
    
    def _save_state(self) -> None:
        """Persist state to disk for high availability."""
        state = {
            "running_jobs": {
                job_id: {
                    "id": job.id,
                    "name": job.name,
                    "priority": job.priority,
                    "submitted_at": job.submitted_at.isoformat(),
                    "status": job.status.value,
                    "metadata": job.metadata,
                    "last_status_change": job.last_status_change.isoformat(),
                    "preemption_count": job.preemption_count,
                    "wait_time_weight": job.wait_time_weight
                }
                for job_id, job in self._running_jobs.items()
            }
        }
        
        # Write to temporary file first to ensure atomic update
        temp_file = f"{self._state_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(state, f, cls=JobJSONEncoder)
        os.rename(temp_file, self._state_file)
    
    def _load_state(self) -> None:
        """Load persisted state for high availability."""
        if not os.path.exists(self._state_file):
            return
            
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            with open(self._state_file, 'r') as f:
                state = json.load(f, object_hook=lambda d: {k: v for k, v in d.items()})
                
            for job_data in state.get("running_jobs", {}).values():
                try:
                    job = Job(
                        id=job_data["id"],
                        name=job_data["name"],
                        priority=job_data["priority"],
                        submitted_at=datetime.fromisoformat(job_data["submitted_at"]),
                        status=JobStatus(job_data["status"]),
                        metadata=job_data["metadata"],
                        last_status_change=datetime.fromisoformat(job_data["last_status_change"]),
                        preemption_count=job_data["preemption_count"],
                        wait_time_weight=job_data["wait_time_weight"]
                    )
                    self._running_jobs[job.id] = job
                except (KeyError, ValueError) as e:
                    logger.error(f"Error loading job data: {e}")
                    continue
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding state file: {e}")
        except FileNotFoundError as e:
            logger.warning(f"State file not found: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading state: {e}")
