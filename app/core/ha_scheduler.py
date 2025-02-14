from typing import Optional, Dict, List
from datetime import datetime, timedelta, timezone
import asyncio
from sqlalchemy import select, update, or_
from app.models.job import Job, JobStatus, JobCreate
from app.models.database import get_session, JobModel, JobStatusEnum
from app.core.scheduler import GlobalMLScheduler
from app.core.queue_manager import QueueManager
import json

class HAGlobalScheduler:
    """High-availability Global ML Scheduler with leader-follower mode."""
    
    # Constants for HA operation
    LEASE_DURATION = 5  # seconds (shorter for testing)
    HEARTBEAT_INTERVAL = 2  # seconds (shorter for testing)
    
    def __init__(self, node_id: str, queue_manager: QueueManager):
        self.node_id = node_id
        self.queue_manager = queue_manager
        self.is_leader = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._scheduler = GlobalMLScheduler(queue_manager)
        self.state_manager = queue_manager.state_manager
        
    async def start(self) -> None:
        """Start the HA scheduler."""
        # Start heartbeat for leader election
        self._heartbeat_task = asyncio.create_task(self._run_heartbeat())
        await self.queue_manager.start()
        
    async def stop(self) -> None:
        """Stop the HA scheduler."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
            self.is_leader = False
        await self.queue_manager.stop()
        
    async def _run_heartbeat(self) -> None:
        """Run leader election heartbeat."""
        while True:
            try:
                # Try to acquire or maintain leadership
                was_leader = self.is_leader
                acquired = await self._try_acquire_leadership()
                
                if acquired:
                    if not was_leader:
                        # Just became leader, sync state
                        await self._sync_state()
                    self.is_leader = True
                    # Leader duties: schedule jobs and update state
                    await self._scheduler.update_priorities()
                    await self._scheduler.schedule()
                else:
                    if was_leader:
                        # Lost leadership, sync state
                        self.is_leader = False
                        await self._sync_state()
                    else:
                        # Not leader, sync state periodically
                        await self._sync_state()
                    
            except Exception as e:
                print(f"Error in heartbeat: {str(e)}")
                self.is_leader = False
                
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            
    async def _try_acquire_leadership(self) -> bool:
        """Try to acquire or maintain leadership."""
        async with get_session() as session:
            try:
                now = datetime.now(timezone.utc)
                lease_expired = now - timedelta(seconds=self.LEASE_DURATION)
                
                # First check if we're already leader
                stmt = select(JobModel).where(JobModel.id == "leader")
                result = await session.execute(stmt)
                leader = result.scalar_one_or_none()
                
                if leader and leader.leader_id == self.node_id:
                    # We're already leader, update heartbeat
                    leader.last_heartbeat = now
                    await session.commit()
                    return True
                
                # Try to acquire leadership
                stmt = (
                    update(JobModel)
                    .where(
                        JobModel.id == "leader",
                        or_(
                            JobModel.leader_id.is_(None),
                            JobModel.last_heartbeat < lease_expired
                        )
                    )
                    .values(
                        leader_id=self.node_id,
                        last_heartbeat=now,
                        status=JobStatusEnum.SUBMITTED,
                        last_status_change=now,
                        job_metadata="{}"  # Ensure metadata is set
                    )
                    .returning(JobModel)
                )
                result = await session.execute(stmt)
                updated = result.scalar_one_or_none()
                
                if updated and updated.leader_id == self.node_id:
                    await session.commit()
                    return True
                    
                # If no leader record exists, create one
                if not leader:
                    leader = JobModel(
                        id="leader",
                        name="leader",
                        priority=0,
                        submitted_at=now,
                        status=JobStatusEnum.SUBMITTED,
                        job_metadata="{}",
                        last_status_change=now,
                        preemption_count=0,
                        wait_time_weight=1.0,
                        leader_id=self.node_id,
                        last_heartbeat=now
                    )
                    session.add(leader)
                    await session.commit()
                    return True
                result = await session.execute(stmt)
                updated = result.scalar_one_or_none()
                
                if updated and updated.leader_id == self.node_id:
                    await session.commit()
                    return True
                
                # If no leader record exists, create one
                stmt = select(JobModel).where(JobModel.id == "leader")
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()
                
                if not existing:
                    try:
                        leader = JobModel(
                            id="leader",
                            name="leader",
                            status=JobStatusEnum.SUBMITTED,
                            priority=0,
                            job_metadata="{}",
                            submitted_at=now,
                            last_status_change=now,
                            leader_id=self.node_id,
                            last_heartbeat=now,
                            preemption_count=0,
                            wait_time_weight=1.0
                        )
                        session.add(leader)
                        await session.commit()
                        self.is_leader = True
                        return True
                    except Exception:
                        await session.rollback()
                        # Another node might have created the leader record
                        # Try to acquire leadership again
                        return await self._try_acquire_leadership()
                
                self.is_leader = False
                return False
                
            except Exception as e:
                print(f"Error in leadership check: {str(e)}")
                await session.rollback()
                return False
                
    async def _sync_state(self) -> None:
        """Sync scheduler state from database."""
        async with get_session() as session:
            # Get all jobs and their states
            result = await session.execute(
                select(JobModel).order_by(JobModel.submitted_at)
            )
            jobs = result.scalars().all()
            
            # Update local state
            self.queue_manager.queue.clear()
            for job in jobs:
                if job.id != "leader" and job.status == JobStatusEnum.SUBMITTED:
                    # Re-queue pending jobs
                    job_create = JobCreate(
                        name=job.name,
                        priority=job.priority,
                        metadata=json.loads(job.job_metadata)
                    )
                    new_job = Job.create(job_create)
                    # Update fields that should be preserved
                    object.__setattr__(new_job, 'id', job.id)
                    object.__setattr__(new_job, 'submitted_at', job.submitted_at)
                    object.__setattr__(new_job, 'last_status_change', job.last_status_change)
                    object.__setattr__(new_job, 'preemption_count', job.preemption_count)
                    object.__setattr__(new_job, 'wait_time_weight', job.wait_time_weight)
                    self.queue_manager.queue.enqueue(new_job)
                    
    async def submit_job(self, job: JobCreate) -> Job:
        """Submit a job to the scheduler."""
        # Check leadership status
        if not self.is_leader and not await self._try_acquire_leadership():
            raise ValueError("Not the leader node")
            
        # Create Job instance from JobCreate
        new_job = Job.create(job)
        
        # Add default cluster if not specified
        if "cluster" not in new_job.metadata:
            metadata = dict(new_job.metadata)
            metadata["cluster"] = "default"
            object.__setattr__(new_job, 'metadata', metadata)
            
        # Save job to database first
        async with get_session() as session:
            db_job = JobModel(
                id=new_job.id,
                name=new_job.name,
                priority=new_job.priority,
                submitted_at=new_job.submitted_at,
                status=JobStatusEnum.SUBMITTED,
                job_metadata=json.dumps(dict(new_job.metadata)),
                last_status_change=new_job.last_status_change,
                preemption_count=new_job.preemption_count,
                wait_time_weight=new_job.wait_time_weight,
                leader_id=None,
                last_heartbeat=None
            )
            session.add(db_job)
            await session.commit()
            
        # Then submit to scheduler
        await self._scheduler.submit_job(new_job)
        return new_job
        
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return await self.queue_manager.get_job(job_id)
