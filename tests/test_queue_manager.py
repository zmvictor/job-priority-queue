import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import update
from app.models.job import JobCreate, JobStatus
from app.core.queue_manager import QueueManager
from app.models.database import get_session, JobModel

class TestQueueManager:
    @pytest.fixture
    async def manager(self):
        # Initialize database
        from app.models.database import init_db
        await init_db()
        
        manager = QueueManager()
        await manager.start()
        # Force leader status for tests
        manager.state_manager.ha_manager.is_leader = True
        manager.state_manager.ha_manager.node_id = "test-leader-node"
        yield manager
        # Cleanup after each test
        await manager.clear()
        await manager.stop()
    
    async def test_job_submission_and_scheduling(self, manager):
        # Create and submit job
        job_create = JobCreate(name="test-job", priority=50)
        job = await manager.submit_job(job_create)
        
        # Verify job was created correctly
        assert job.name == "test-job"
        assert job.priority == 50
        assert job.status == JobStatus.SUBMITTED
        
        # Schedule the job
        scheduled_job = await manager.schedule_next_job()
        assert scheduled_job.id == job.id
        assert scheduled_job.status == JobStatus.RUNNING
    
    async def test_priority_with_wait_time(self, manager):
        # Create two jobs with same priority
        job1 = await manager.submit_job(JobCreate(name="job1", priority=50))
        job2 = await manager.submit_job(JobCreate(name="job2", priority=50))
        
        # Artificially age job1
        job1.submitted_at -= timedelta(hours=12)
        
        # Update job in database
        async with get_session() as session:
            stmt = (
                update(JobModel)
                .where(JobModel.id == job1.id)
                .values(submitted_at=job1.submitted_at)
            )
            await session.execute(stmt)
            await session.commit()
        
        # Update priorities
        await manager.update_priorities()
        
        # The older job should be scheduled first
        scheduled = await manager.schedule_next_job()
        assert scheduled.id == job1.id
    
    async def test_preemption_flow(self, manager):
        # Submit and schedule first job
        job1 = await manager.submit_job(JobCreate(name="job1", priority=50))
        await manager.schedule_next_job()
        
        # Submit higher priority job
        job2 = await manager.submit_job(JobCreate(name="job2", priority=90))
        
        # Preempt job1
        preempted = await manager.preempt_job(job1.id)
        assert preempted.status == JobStatus.PREEMPTED
        assert preempted.preemption_count == 1
        
        # Ensure all datetimes are timezone-aware
        preempted.submitted_at = preempted.submitted_at.replace(tzinfo=timezone.utc)
        preempted.last_status_change = preempted.last_status_change.replace(tzinfo=timezone.utc)
        
        # job2 should be scheduled next
        scheduled = await manager.schedule_next_job()
        assert scheduled.id == job2.id
        
        # Original job should be requeued
        assert await manager.get_job(job1.id) is not None
    
    async def test_running_jobs_tracking(self, manager):
        # Submit and schedule multiple jobs
        jobs = []
        for i in range(3):
            job = await manager.submit_job(JobCreate(name=f"job{i}", priority=50))
            jobs.append(job)
        
        # Schedule all jobs
        for _ in range(3):
            await manager.schedule_next_job()
        
        # Verify running jobs
        running = await manager.get_running_jobs()
        assert len(running) == 3
        assert all(job.status == JobStatus.RUNNING for job in running)
    
    async def test_long_running_job_priority(self, manager):
        # Submit jobs with same priority
        jobs = []
        for i in range(3):
            job = await manager.submit_job(JobCreate(name=f"job{i}", priority=50))
            jobs.append(job)
        
        # Age jobs differently
        jobs[0].submitted_at = jobs[0].submitted_at - timedelta(hours=24)  # Very old
        jobs[1].submitted_at = jobs[1].submitted_at - timedelta(hours=12)  # Moderately old
        # jobs[2] stays recent
        
        # Update jobs in database
        async with get_session() as session:
            for job in jobs[:2]:  # Only update aged jobs
                stmt = (
                    update(JobModel)
                    .where(JobModel.id == job.id)
                    .values(submitted_at=job.submitted_at)
                )
                await session.execute(stmt)
            await session.commit()
        
        # Update priorities
        await manager.update_priorities()
        
        # Jobs should be scheduled in order of wait time
        scheduled = await manager.schedule_next_job()
        assert scheduled.id == jobs[0].id  # Oldest first
        
        scheduled = await manager.schedule_next_job()
        assert scheduled.id == jobs[1].id  # Second oldest
        
        scheduled = await manager.schedule_next_job()
        assert scheduled.id == jobs[2].id  # Most recent last
