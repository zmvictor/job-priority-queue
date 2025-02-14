import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobCreate, JobStatus
from app.core.ha_scheduler import HAGlobalScheduler
from app.core.queue_manager import QueueManager
from app.models.database import get_session, JobModel

@pytest.fixture
async def queue_manager():
    manager = QueueManager()
    await manager.start()
    yield manager
    await manager.stop()
    
@pytest.fixture
async def ha_scheduler(queue_manager):
    scheduler = HAGlobalScheduler("test-node-1", queue_manager)
    await scheduler.start()
    yield scheduler
    await scheduler.stop()
    
def create_test_job(priority: int = 50) -> Job:
    """Create a test job with given priority."""
    job_create = JobCreate(
        name=f"test-job-{priority}",
        priority=priority,
        metadata={}
    )
    return Job.create(job_create)

class TestHAGlobalScheduler:
    async def test_leader_election(self, ha_scheduler):
        """Test leader election process."""
        # Initially should not be leader
        assert not ha_scheduler.is_leader
        
        # After heartbeat should become leader (since no other nodes)
        await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
        assert ha_scheduler.is_leader
        
        # Create another scheduler
        other_scheduler = HAGlobalScheduler("test-node-2", ha_scheduler.queue_manager)
        await other_scheduler.start()
        
        try:
            # Wait for heartbeat
            await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
            
            # Only one should be leader
            assert ha_scheduler.is_leader != other_scheduler.is_leader
        finally:
            await other_scheduler.stop()
            
    async def test_leader_failover(self, ha_scheduler):
        """Test leader failover when leader fails."""
        # Wait to become leader
        await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
        assert ha_scheduler.is_leader
        
        # Create follower
        follower = HAGlobalScheduler("test-node-2", ha_scheduler.queue_manager)
        await follower.start()
        
        try:
            # Stop leader
            await ha_scheduler.stop()
            
            # Wait for failover
            await asyncio.sleep(ha_scheduler.LEASE_DURATION + 1)
            
            # Follower should become leader
            assert follower.is_leader
        finally:
            await follower.stop()
            
    async def test_state_sync(self, ha_scheduler):
        """Test state synchronization between leader and follower."""
        # Wait to become leader
        await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
        assert ha_scheduler.is_leader
        
        # Submit jobs as leader
        jobs = [create_test_job(priority=p) for p in [50, 60, 70]]
        for job in jobs:
            await ha_scheduler.submit_job(job)
            
        # Create follower
        follower = HAGlobalScheduler("test-node-2", ha_scheduler.queue_manager)
        await follower.start()
        
        try:
            # Wait for sync
            await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
            
            # Verify follower has synced state
            for job in jobs:
                follower_job = await follower.get_job(job.id)
                assert follower_job is not None
                assert follower_job.priority == job.priority
                assert follower_job.status == job.status
        finally:
            await follower.stop()
            
    async def test_leader_only_operations(self, ha_scheduler):
        """Test that only leader can perform write operations."""
        # Create follower
        follower = HAGlobalScheduler("test-node-2", ha_scheduler.queue_manager)
        await follower.start()
        
        try:
            # Wait for leader election
            await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
            
            # Try operations on both nodes
            job = create_test_job()
            
            if ha_scheduler.is_leader:
                leader, non_leader = ha_scheduler, follower
            else:
                leader, non_leader = follower, ha_scheduler
                
            # Leader should succeed
            await leader.submit_job(job)
            
            # Non-leader should fail
            with pytest.raises(ValueError, match="Not the leader node"):
                await non_leader.submit_job(job)
                
            # Both should be able to read
            leader_job = await leader.get_job(job.id)
            follower_job = await non_leader.get_job(job.id)
            
            assert leader_job is not None
            assert follower_job is not None
            assert leader_job.id == follower_job.id
        finally:
            await follower.stop()
            
    async def test_database_persistence(self, ha_scheduler):
        """Test that state is persisted in database."""
        # Wait to become leader
        await asyncio.sleep(ha_scheduler.HEARTBEAT_INTERVAL + 1)
        assert ha_scheduler.is_leader
        
        # Submit job
        job = create_test_job()
        await ha_scheduler.submit_job(job)
        
        # Verify in database
        async with get_session() as session:
            result = await session.execute(
                select(JobModel).where(JobModel.id == job.id)
            )
            db_job = result.scalar_one()
            assert db_job is not None
            assert db_job.priority == job.priority
            assert db_job.status == job.status.value
