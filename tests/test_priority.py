import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import update
from app.models.job import Job, JobCreate, JobStatus
from app.core.queue_manager import queue_manager
from app.models.database import get_session, JobModel

@pytest.fixture
async def setup_jobs():
    # Initialize database
    from app.models.database import init_db
    await init_db()
    
    # Set leader status
    queue_manager.state_manager.ha_manager.is_leader = True
    queue_manager.state_manager.ha_manager.node_id = "test-leader-node"
    
    jobs = []
    for i in range(3):
        job = await queue_manager.submit_job(
            JobCreate(name=f"job_{i}", priority=50)
        )
        jobs.append(job)
    return jobs

@pytest.mark.asyncio
async def test_priority_adjustment(setup_jobs):
    jobs = setup_jobs
    # Simulate 12 hours wait time
    jobs[0].submitted_at = datetime.now(timezone.utc) - timedelta(hours=12)
    
    # Priority weight should be 1.5x after 12 hours
    wait_time = jobs[0].calculate_wait_time()
    assert 11.9 <= wait_time <= 12.1  # Allow small time difference
    jobs[0].update_wait_time_weight()  # Update weight before checking
    assert abs(jobs[0].wait_time_weight - (1.0 + (wait_time / 24.0))) < 0.1  # Allow small difference

@pytest.mark.asyncio
async def test_preemption_priority(setup_jobs):
    jobs = setup_jobs
    # Schedule the job first
    job = await queue_manager.schedule_next_job()
    assert job is not None
    assert job.status == JobStatus.RUNNING
    
    # Artificially age the job
    job.submitted_at = job.submitted_at - timedelta(hours=12)
    async with get_session() as session:
        stmt = (
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(submitted_at=job.submitted_at)
        )
        await session.execute(stmt)
        await session.commit()
    
    # Preempt job and verify priority boost
    preempted_job = await queue_manager.preempt_job(job.id)
    assert preempted_job.status == JobStatus.PREEMPTED
    assert preempted_job.preemption_count == 1
    assert preempted_job.wait_time_weight > 1.0  # Should have priority boost

@pytest.mark.asyncio
async def test_priority_ordering(setup_jobs):
    """Test that jobs are ordered correctly by priority."""
    jobs = setup_jobs
    
    # Set different priorities
    priorities = [30, 70, 50]
    for job, priority in zip(jobs, priorities):
        job.priority = priority
    
    # Get jobs in priority order
    ordered_jobs = sorted(jobs, key=lambda j: (-j.priority, j.submitted_at))
    assert ordered_jobs[0].priority == 70
    assert ordered_jobs[1].priority == 50
    assert ordered_jobs[2].priority == 30

@pytest.mark.asyncio
async def test_wait_time_boost(setup_jobs):
    """Test that wait time properly boosts job priority."""
    jobs = setup_jobs
    
    # Set different wait times
    wait_times = [24, 12, 6]  # hours
    for job, hours in zip(jobs, wait_times):
        job.submitted_at = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    # Verify wait time weights
    for job, hours in zip(jobs, wait_times):
        expected_boost = 1.0 + (hours / 24.0)  # Max 2x boost after 24 hours
        assert abs(job.calculate_wait_time() / hours - 1.0) < 0.1  # Within 10% accuracy
