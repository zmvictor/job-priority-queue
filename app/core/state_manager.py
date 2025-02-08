import json
import uuid
from datetime import datetime, UTC
from typing import List, Optional, Dict
from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from app.models.database import JobModel, JobStatusEnum, get_session
from app.models.job import Job, JobStatus
from app.core.ha import HAManager

class HAJobStateManager:
    """High-availability job state manager using SQLite."""
    
    def __init__(self, node_id: Optional[str] = None):
        self.node_id = node_id or str(uuid.uuid4())
        self.ha_manager = HAManager(self.node_id)
    
    async def start(self):
        """Start the HA manager and reconcile state."""
        await self.ha_manager.start_heartbeat()
        async with get_session() as session:
            await self.ha_manager.reconcile_state(session)
    
    async def stop(self):
        """Stop the HA manager."""
        await self.ha_manager.stop_heartbeat()
    
    async def transition_to_running(self, job: Job, session: AsyncSession) -> Optional[Job]:
        """Thread-safe transition to running state."""
        if not self.ha_manager.is_leader:
            return None
            
        now = datetime.now(UTC)
        
        try:
            # First verify the job exists and is in SUBMITTED state
            stmt = (
                select(JobModel)
                .where(JobModel.id == job.id)
                .with_for_update()
            )
            result = await session.execute(stmt)
            db_job = result.scalar_one_or_none()
            
            if not db_job:
                raise ValueError(f"Job {job.id} not found")
                
            if db_job.status not in [JobStatusEnum.SUBMITTED, JobStatusEnum.PREEMPTED]:
                raise ValueError(f"Job {job.id} must be in SUBMITTED or PREEMPTED state to transition to RUNNING (current status: {db_job.status})")
            
            # Update database state and get updated row in single transaction
            stmt = (
                update(JobModel)
                .where(JobModel.id == job.id)
                .values(
                    status=JobStatusEnum.RUNNING,
                    last_status_change=now,
                    leader_id=self.node_id
                )
                .returning(JobModel)
            )
            result = await session.execute(stmt)
            updated_job = result.scalar_one_or_none()
            
            if not updated_job:
                raise ValueError(f"Failed to transition job {job.id} to running state")
            
            # Update in-memory state from database
            job.status = JobStatus(updated_job.status.value)
            job.last_status_change = updated_job.last_status_change
            job.leader_id = updated_job.leader_id
            
            await session.commit()
            return job
        except Exception as e:
            await session.rollback()
            raise ValueError(f"Failed to transition job {job.id} to running state: {str(e)}")
    
    async def preempt_job(self, job: Job, session: AsyncSession) -> Job:
        """Thread-safe job preemption."""
        import logging
        logger = logging.getLogger(__name__)
        
        if not self.ha_manager.is_leader:
            raise ValueError("Not the leader node")
            
        now = datetime.now(UTC)
        logger.info(f"Attempting to preempt job {job.id} with current status {job.status}")
        
        try:
            # First verify the job exists and is in RUNNING state
            stmt = (
                select(JobModel)
                .where(JobModel.id == job.id)
                .with_for_update()
            )
            result = await session.execute(stmt)
            db_job = result.scalar_one_or_none()
            
            if not db_job:
                logger.error(f"Job {job.id} not found in database")
                raise ValueError(f"Job {job.id} not found")
                
            logger.info(f"Found job {job.id} in database with status {db_job.status}")
                
            if db_job.status != JobStatusEnum.RUNNING:
                logger.error(f"Job {job.id} is not in RUNNING state (current status: {db_job.status})")
                raise ValueError(f"Job {job.id} is not in RUNNING state (current status: {db_job.status})")
                
            # Update the job status
            stmt = (
                update(JobModel)
                .where(JobModel.id == job.id)
                .values(
                    status=JobStatusEnum.PREEMPTED,
                    last_status_change=now,
                    leader_id=None,
                    preemption_count=JobModel.preemption_count + 1,
                    wait_time_weight=1.0 + ((now - db_job.submitted_at.astimezone(UTC)).total_seconds() / 86400.0)
                )
                .returning(JobModel)
            )
            result = await session.execute(stmt)
            updated_job = result.scalar_one_or_none()
            
            if not updated_job:
                logger.error(f"Failed to update job {job.id} in database")
                raise ValueError(f"Failed to preempt job {job.id}")
            
            logger.info(f"Successfully updated job {job.id} to PREEMPTED state")
            
            # Create a new job object with the latest state from the database
            job = Job.model_validate({
                'id': updated_job.id,
                'name': updated_job.name,
                'priority': updated_job.priority,
                'submitted_at': updated_job.submitted_at,
                'status': JobStatus(updated_job.status.value),
                'metadata': json.loads(updated_job.job_metadata),
                'last_status_change': updated_job.last_status_change,
                'preemption_count': updated_job.preemption_count,
                'wait_time_weight': updated_job.wait_time_weight,
                'leader_id': updated_job.leader_id
            })
            
            logger.info(f"Successfully created new job object for {job.id}")
            return job
        except Exception as e:
            logger.error(f"Error preempting job {job.id}: {str(e)}")
            raise ValueError(f"Failed to preempt job {job.id}: {str(e)}")
    
    async def get_running_jobs(self, session: AsyncSession) -> List[Job]:
        """Thread-safe access to running jobs."""
        stmt = (
            select(JobModel)
            .where(JobModel.status == JobStatusEnum.RUNNING)
            .where(JobModel.leader_id == self.node_id)
        )
        result = await session.execute(stmt)
        jobs = result.scalars().all()
        return [
            Job.model_validate({
                'id': job.id,
                'name': job.name,
                'priority': job.priority,
                'submitted_at': job.submitted_at,
                'status': JobStatus(job.status.value),
                'metadata': json.loads(job.job_metadata),
                'last_status_change': job.last_status_change,
                'preemption_count': job.preemption_count,
                'wait_time_weight': job.wait_time_weight,
                'leader_id': job.leader_id
            })
            for job in jobs
        ]
    
    async def get_job_state(self, job_id: str, session: AsyncSession) -> Optional[Job]:
        """Thread-safe job state lookup."""
        stmt = select(JobModel).where(JobModel.id == job_id)
        result = await session.execute(stmt)
        job = result.scalar_one_or_none()
        if not job:
            return None
            
        return Job.model_validate({
            'id': job.id,
            'name': job.name,
            'priority': job.priority,
            'submitted_at': job.submitted_at,
            'status': JobStatus(job.status.value),
            'metadata': json.loads(job.job_metadata),
            'last_status_change': job.last_status_change,
            'preemption_count': job.preemption_count,
            'wait_time_weight': job.wait_time_weight,
            'leader_id': job.leader_id
        })
        
    async def clear(self) -> None:
        """Clear all job state."""
        session = get_session()
        async with session as s:
            await s.execute(text("DELETE FROM jobs"))
            await s.commit()
