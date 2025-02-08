import asyncio
from datetime import datetime, timedelta, UTC
from typing import Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.database import JobModel, async_session, get_session

class HAManager:
    """Manages high availability for the job queue."""
    
    def __init__(self, node_id: str, lease_duration: int = 30):
        self.node_id = node_id
        self.lease_duration = lease_duration
        self.is_leader = False
        self._heartbeat_task: Optional[asyncio.Task] = None
    
    async def try_acquire_leadership(self, session: AsyncSession) -> bool:
        """Try to acquire leadership using SQLite row locking."""
        try:
            # Use SQLite's row locking to implement leader election
            result = await session.execute(
                text("""
                UPDATE jobs 
                SET leader_id = :node_id, 
                    last_status_change = :now
                WHERE leader_id IS NULL 
                   OR last_status_change < :expired
                """),
                {
                    "node_id": self.node_id,
                    "now": datetime.now(UTC),
                    "expired": datetime.now(UTC) - timedelta(seconds=self.lease_duration)
                }
            )
            await session.commit()
            
            # If we updated any rows, we're the leader
            self.is_leader = result.rowcount > 0
            return self.is_leader
        except Exception:
            self.is_leader = False
            return False
    
    async def start_heartbeat(self):
        """Start sending heartbeats to maintain leadership."""
        async def heartbeat():
            while True:
                try:
                    async with async_session() as session:
                        await self.try_acquire_leadership(session)
                except Exception:
                    self.is_leader = False
                await asyncio.sleep(self.lease_duration // 2)
        
        self._heartbeat_task = asyncio.create_task(heartbeat())
    
    async def stop_heartbeat(self):
        """Stop sending heartbeats."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
            self.is_leader = False
    
    async def reconcile_state(self, session: AsyncSession):
        """Reconcile job states after becoming leader."""
        # Find jobs that were running on failed nodes
        stale_jobs = await session.execute(
            text("""
            SELECT * FROM jobs 
            WHERE status = 'running' 
              AND (leader_id != :node_id OR leader_id IS NULL)
              AND last_status_change < :expired
            """),
            {
                "node_id": self.node_id,
                "expired": datetime.now(UTC) - timedelta(seconds=self.lease_duration)
            }
        )
        
        # Requeue these jobs
        for job in stale_jobs:
            job.status = "pending"
            job.leader_id = None
            job.last_status_change = datetime.now(UTC)
        
        await session.commit()
