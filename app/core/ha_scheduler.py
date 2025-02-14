from typing import Optional, Dict, List
from datetime import datetime, timedelta, timezone
import asyncio
from sqlalchemy import select, update
from app.models.job import Job, JobStatus
from app.models.database import get_session, JobModel
from app.core.scheduler import GlobalMLScheduler
from app.core.queue_manager import QueueManager

class HAGlobalScheduler:
    """High-availability Global ML Scheduler with leader-follower mode."""
    
    # Constants for HA operation
    LEASE_DURATION = 30  # seconds
    HEARTBEAT_INTERVAL = 15  # seconds
    
    def __init__(self, node_id: str, queue_manager: QueueManager):
        self.node_id = node_id
        self.queue_manager = queue_manager
        self.is_leader = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._scheduler = GlobalMLScheduler(queue_manager)
        
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
                # Update or insert leader record
                stmt = update(JobModel).where(
                    JobModel.id == "leader",
                    (
                        JobModel.leader_id.is_(None) |
                        (JobModel.leader_id == self.node_id) |
                        (JobModel.last_heartbeat < datetime.now(timezone.utc) - timedelta(seconds=self.LEASE_DURATION))
                    )
                ).values(
                    leader_id=self.node_id,
                    last_heartbeat=datetime.now(timezone.utc)
                )
                result = await session.execute(stmt)
                await session.commit()
                
                # Check if we got/maintained leadership
                return result.rowcount > 0
                
            except Exception:
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
                    self.queue_manager.queue.enqueue(Job(
                        id=job.id,
                        name=job.name,
                        priority=job.priority,
                        status=JobStatus(job.status.value),
                        submitted_at=job.submitted_at,
                        last_status_change=job.last_status_change,
                        metadata=job.metadata,
                        preemption_count=job.preemption_count,
                        wait_time_weight=job.wait_time_weight
                    ))
                    
    async def submit_job(self, job: Job) -> None:
        """Submit a job to the scheduler."""
        if not self.is_leader:
            raise ValueError("Not the leader node")
        await self._scheduler.submit_job(job)
        
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return await self.queue_manager.get_job(job_id)
