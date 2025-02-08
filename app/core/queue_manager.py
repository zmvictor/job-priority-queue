from typing import Optional, List
from fastapi import HTTPException
import json
from sqlalchemy import update, select
from app.models.job import Job, JobCreate, JobStatus
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
                job_metadata=json.dumps(job.metadata),
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
        async with get_session() as session:
            # Check running jobs first
            job = await self.state_manager.get_job_state(job_id, session)
            if job:
                return job
            # Check queue
            return self.queue.get_job(job_id)
    
    async def preempt_job(self, job: Job, session) -> Job:
        """Preempt a running job."""
        try:
            # Re-queue the preempted job with its current wait time
            updated_job = await self.state_manager.preempt_job(job, session)
            if updated_job:
                # Update priority based on wait time before re-queueing
                updated_job.wait_time_weight = 1.0 + (updated_job.calculate_wait_time() / 24.0)  # Boost based on wait time
                self.queue.enqueue(updated_job)
                return updated_job
            raise ValueError(f"Failed to preempt job {job.id}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error while preempting job: {str(e)}")
    
    async def schedule_next_job(self) -> Optional[Job]:
        """Schedule the next job from the queue."""
        job = self.queue.dequeue()
        if job:
            async with get_session() as session:
                try:
                    # Transition job to running state
                    updated_job = await self.state_manager.transition_to_running(job, session)
                    await session.commit()
                    if updated_job and updated_job.status == JobStatus.RUNNING:
                        return updated_job
                except Exception as e:
                    # If job wasn't transitioned to running, put it back in queue
                    self.queue.enqueue(job)
                    raise e
        return None
    
    async def get_running_jobs(self) -> List[Job]:
        """Get all currently running jobs."""
        async with get_session() as session:
            return await self.state_manager.get_running_jobs(session)
    
    async def update_priorities(self) -> None:
        """Update priorities of all queued jobs based on wait time."""
        # Get all jobs from queue
        jobs = []
        while (job := self.queue.dequeue()):
            jobs.append(job)
            
        # Update job states in database
        async with get_session() as session:
            for job in jobs:
                stmt = (
                    update(JobModel)
                    .where(JobModel.id == job.id)
                    .values(wait_time_weight=1.0 + (job.calculate_wait_time() / 24.0))
                )
                await session.execute(stmt)
            await session.commit()
        
        # Re-enqueue with updated priorities
        for job in jobs:
            self.queue.enqueue(job)
    
    async def clear(self) -> None:
        """Clear all jobs from queue and state manager."""
        self.queue.clear()
        await self.state_manager.clear()

# Global queue manager instance
queue_manager = QueueManager()
