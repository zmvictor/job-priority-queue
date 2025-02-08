from typing import Optional, List
from fastapi import HTTPException
import json
from datetime import datetime, UTC
from sqlalchemy import update, select
from app.models.job import Job, JobCreate, JobStatus, JobJSONEncoder
from app.models.queue import PriorityQueue
from app.core.state_manager import HAJobStateManager
from app.models.database import get_session, JobModel, JobStatusEnum

class QueueManager:
    def __init__(self):
        self.queue = PriorityQueue()
        self.state_manager = HAJobStateManager()
        
    async def start(self):
        """Start the HA manager."""
        await self.state_manager.start()
        
    async def stop(self):
        """Stop the HA manager."""
        await self.state_manager.stop()
    
    async def submit_job(self, job_create: JobCreate) -> Job:
        """Submit a new job to the queue."""
        job = Job.create(job_create)
        async with get_session() as session:
            # Create job in database
            db_job = JobModel(
                id=job.id,
                name=job.name,
                priority=job.priority,
                submitted_at=job.submitted_at,
                status=JobStatusEnum(job.status.value),
                job_metadata=json.dumps(job.metadata, cls=JobJSONEncoder),
                last_status_change=job.last_status_change,
                preemption_count=job.preemption_count,
                wait_time_weight=job.wait_time_weight
            )
            session.add(db_job)
            await session.commit()
            
        self.queue.enqueue(job)
        return job
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID from either queue or running jobs."""
        # Check running jobs first
        job = await self.state_manager.get_job_state(job_id)
        if job:
            return job
        # Check queue
        return self.queue.get_job(job_id)
    
    async def preempt_job(self, job_id: str) -> Job:
        """Preempt a running job."""
        try:
            # Get the job first
            job = await self.get_job(job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            # Re-queue the preempted job with its current wait time
            updated_job = await self.state_manager.preempt_job(job)
            if updated_job:
                # Update priority based on wait time before re-queueing
                updated_job.update_wait_time_weight()  # Use the Job model's method
                self.queue.enqueue(updated_job)
                return updated_job
            raise ValueError(f"Failed to preempt job {job_id}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error while preempting job: {str(e)}")
    
    def _sort_jobs_by_priority(self, jobs: List[Job]) -> None:
        """Sort jobs by priority and wait time."""
        # Update wait time weights before sorting
        for job in jobs:
            job.update_wait_time_weight()
            
        # Sort jobs by effective priority (base priority * wait time weight)
        jobs.sort(key=lambda j: (
            -j.priority,  # Base priority first
            -j.wait_time_weight,  # Then by wait time weight
            j.submitted_at.timestamp()  # Finally by submission time
        ))

    async def schedule_next_job(self) -> Optional[Job]:
        """Schedule the next job from the queue."""
        # Get all jobs and sort by priority and wait time
        jobs = []
        while (job := self.queue.dequeue()):
            jobs.append(job)
            
        if not jobs:
            return None
            
        # Sort jobs by priority and wait time
        self._sort_jobs_by_priority(jobs)
        
        # Try to schedule the highest priority job
        job = jobs[0]
        try:
            # Transition job to running state
            updated_job = await self.state_manager.transition_to_running(job)
            if updated_job and updated_job.status == JobStatus.RUNNING:
                # Re-enqueue remaining jobs
                for remaining_job in jobs[1:]:
                    self.queue.enqueue(remaining_job)
                return updated_job
        except Exception as e:
            # If job wasn't transitioned to running, put all jobs back in queue
            for j in jobs:
                self.queue.enqueue(j)
            raise e
            
        # Re-enqueue all jobs if scheduling failed
        for j in jobs:
            self.queue.enqueue(j)
        return None
    
    async def get_running_jobs(self) -> List[Job]:
        """Get all currently running jobs."""
        return await self.state_manager.get_running_jobs()
    
    async def update_priorities(self) -> None:
        """Update priorities of all queued jobs based on wait time."""
        # Get all jobs from queue
        jobs = []
        while (job := self.queue.dequeue()):
            jobs.append(job)
            
        if not jobs:
            return
            
        # Update job states in database and wait time weights
        async with get_session() as session:
            for job in jobs:
                # Update wait time weight based on current wait time
                job.update_wait_time_weight()
                # First get the current job from DB to preserve timestamps
                result = await session.execute(
                    select(JobModel).where(JobModel.id == job.id)
                )
                db_job = result.scalar_one()
                
                # Update job with DB timestamps to calculate correct weight
                job.submitted_at = db_job.submitted_at
                job.last_status_change = db_job.last_status_change
                job.update_wait_time_weight()
                
                # Update only the weight in DB
                stmt = (
                    update(JobModel)
                    .where(JobModel.id == job.id)
                    .values(wait_time_weight=job.wait_time_weight)
                )
                await session.execute(stmt)
            await session.commit()
            
        # Sort jobs by priority and wait time
        self._sort_jobs_by_priority(jobs)
            
        # Re-enqueue jobs in priority and wait time order
        for job in jobs:
            self.queue.enqueue(job)
    
    async def clear(self) -> None:
        """Clear all jobs from queue and state manager."""
        self.queue.clear()
        await self.state_manager.clear()

# Global queue manager instance
queue_manager = QueueManager()
