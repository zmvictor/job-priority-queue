from typing import Optional, Dict, List
from datetime import datetime, timedelta, timezone
import asyncio
from sqlalchemy import select, update
from app.models.job import Job, JobStatus, JobCreate
from app.models.database import get_session, JobModel
from app.core.scheduler import GlobalMLScheduler
from app.core.queue_manager import QueueManager

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
        await self.queue_manager.stop()
        
    async def _run_heartbeat(self) -> None:
        """Run leader election heartbeat."""
        while True:
            try:
                # Try to acquire or maintain leadership
                self.is_leader = await self._try_acquire_leadership()
                
                if self.is_leader:
                    # Leader duties: schedule jobs and update state
                    await self._scheduler.update_priorities()
                    await self._scheduler.schedule()
                else:
                    # Follower duties: sync state from database
                    await self._sync_state()
                    
            except Exception as e:
                print(f"Error in heartbeat: {str(e)}")
                self.is_leader = False
                
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            
    async def _try_acquire_leadership(self) -> bool:
        """Try to acquire or maintain leadership."""
        async with get_session() as session:
            try:
                # Get current leader record
                stmt = select(JobModel).where(JobModel.id == "leader")
                result = await session.execute(stmt)
                leader = result.scalar_one_or_none()
                
                if not leader:
                    return False
                
                # Check if we can take leadership
                now = datetime.now(timezone.utc)
                can_take_leadership = (
                    leader.leader_id is None or
                    leader.leader_id == self.node_id or
                    (leader.last_heartbeat and leader.last_heartbeat < now - timedelta(seconds=self.LEASE_DURATION))
                )
                
                if can_take_leadership:
                    # Update leader record
                    leader.leader_id = self.node_id
                    leader.last_heartbeat = now
                    await session.commit()
                    return True
                    
                return False
                
            except Exception as e:
                print(f"Error in leadership check: {str(e)}")
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
                if job.status == JobStatus.PENDING:
                    # Re-queue pending jobs
                    job_create = JobCreate(
                        name=job.name,
                        priority=job.priority,
                        metadata=dict(job.metadata)
                    )
                    new_job = Job.create(job_create)
                    # Update fields that should be preserved
                    object.__setattr__(new_job, 'id', job.id)
                    object.__setattr__(new_job, 'submitted_at', job.submitted_at)
                    object.__setattr__(new_job, 'last_status_change', job.last_status_change)
                    object.__setattr__(new_job, 'preemption_count', job.preemption_count)
                    object.__setattr__(new_job, 'wait_time_weight', job.wait_time_weight)
                    self.queue_manager.queue.enqueue(new_job)
                    
    async def submit_job(self, job: Job) -> None:
        """Submit a job to the scheduler."""
        if not self.is_leader:
            raise ValueError("Not the leader node")
        await self._scheduler.submit_job(job)
        
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return await self.queue_manager.get_job(job_id)
