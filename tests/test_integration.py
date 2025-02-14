import pytest
import asyncio
from datetime import datetime, timedelta, timezone
import httpx
from fastapi import FastAPI
from sqlalchemy import update, select
from app.main import app
from app.models.job import JobCreate, JobStatus
from app.core.queue_manager import queue_manager
from app.models.database import get_session, JobModel, JobStatusEnum

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def client():
    """Create async test client."""
    async with httpx.AsyncClient(base_url="http://test", transport=httpx.ASGITransport(app=app)) as client:
        yield client

@pytest.fixture(autouse=True)
async def setup_and_cleanup():
    """Setup test environment and cleanup after."""
    # Initialize database
    from app.models.database import init_db
    await init_db()
    
    # Start HA manager
    await queue_manager.start()
    # Force leader status for tests
    queue_manager.state_manager.ha_manager.is_leader = True
    # Clear any existing state
    await queue_manager.clear()
    yield
    # Cleanup
    await queue_manager.stop()

class TestJobLifecycle:
    @pytest.fixture
    async def client(self, test_client):
        """Get a test client."""
        return test_client
        
    async def test_full_job_lifecycle(self, client):
        """Test the complete lifecycle of a job through the API."""
        # Submit job
        job_data = {"name": "training-job", "priority": 50}
        response = await client.post("/api/v1/jobs", json=job_data)
        assert response.status_code == 200
        job_id = response.json()["id"]
        
        # Verify job status
        response = await client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json()["status"] == JobStatus.SUBMITTED.value
        
        # Schedule job
        response = await client.post("/api/v1/jobs/schedule")
        assert response.status_code == 200
        scheduled_job = response.json()
        assert scheduled_job["id"] == job_id
        assert scheduled_job["status"] == JobStatus.RUNNING.value
        
        # Verify running jobs list
        response = await client.get("/api/v1/jobs")
        assert response.status_code == 200
        running_jobs = response.json()
        assert len(running_jobs) == 1
        assert running_jobs[0]["id"] == job_id

    async def test_preemption_with_long_running_job(self, client):
        """Test preemption of long-running jobs with priority and wait time."""
        # Submit and schedule first job
        job1_data = {"name": "long-running-job", "priority": 50}
        response = await client.post("/api/v1/jobs", json=job1_data)
        assert response.status_code == 200
        job1_id = response.json()["id"]
        
        # Schedule job1
        response = await client.post("/api/v1/jobs/schedule")
        assert response.status_code == 200
        scheduled_job = response.json()
        assert scheduled_job["id"] == job1_id
        assert scheduled_job["status"] == JobStatus.RUNNING.value
        
        # Verify job1 is running
        response = await client.get(f"/api/v1/jobs/{job1_id}")
        assert response.status_code == 200
        job1 = response.json()
        assert job1["status"] == JobStatus.RUNNING.value
        
        # Get running jobs to verify
        response = await client.get("/api/v1/jobs")
        assert response.status_code == 200
        running_jobs = response.json()
        assert len(running_jobs) == 1
        assert running_jobs[0]["id"] == job1_id
        assert running_jobs[0]["status"] == JobStatus.RUNNING.value
        
        # Artificially age the job
        job1 = await queue_manager.get_job(job1_id)
        assert job1 is not None
        async with get_session() as session:
            stmt = (
                update(JobModel)
                .where(JobModel.id == job1_id)
                .values(submitted_at=job1.submitted_at - timedelta(hours=12))
            )
            await session.execute(stmt)
            await session.commit()
            job1.submitted_at = job1.submitted_at - timedelta(hours=12)
        
        # Submit higher priority job
        job2_data = {"name": "high-priority-job", "priority": 90}
        response = await client.post("/api/v1/jobs", json=job2_data)
        job2_id = response.json()["id"]
        
        # Preempt job1
        response = await client.post(f"/api/v1/jobs/{job1_id}/preempt")
        assert response.status_code == 200
        assert response.json()["status"] == JobStatus.PREEMPTED.value
        
        # Schedule job2
        response = await client.post("/api/v1/jobs/schedule")
        assert response.status_code == 200
        scheduled_job = response.json()
        assert scheduled_job["id"] == job2_id
        
        # Verify job1 is requeued with higher priority due to wait time
        await client.post("/api/v1/jobs/update-priorities")
        response = await client.post("/api/v1/jobs/schedule")
        assert response.status_code == 200
        scheduled_job = response.json()
        assert scheduled_job["id"] == job1_id  # Should be scheduled next due to wait time

    async def test_concurrent_job_submissions(self, client):
        """Test concurrent job submissions and scheduling."""
        async def submit_and_schedule(name: str, priority: int):
            response = await client.post("/api/v1/jobs", json={"name": name, "priority": priority})
            return response.json()["id"]
        
        # Submit multiple jobs concurrently
        job_ids = await asyncio.gather(*[
            submit_and_schedule(f"job-{i}", 50)
            for i in range(5)
        ])
        
        # Schedule all jobs
        scheduled_jobs = []
        for _ in range(5):
            response = await client.post("/api/v1/jobs/schedule")
            assert response.status_code == 200
            scheduled_jobs.append(response.json())
        
        # Verify all jobs were scheduled
        assert len(scheduled_jobs) == 5
        assert all(job["status"] == JobStatus.RUNNING.value for job in scheduled_jobs)
        
        # Verify running jobs count
        response = await client.get("/api/v1/jobs")
        assert response.status_code == 200
        assert len(response.json()) == 5

    async def test_priority_updates_with_wait_time(self, client):
        """Test that jobs get priority boost based on wait time."""
        # Submit jobs with same priority but different timestamps
        jobs = []
        for i in range(3):
            response = await client.post("/api/v1/jobs", json={"name": f"job-{i}", "priority": 50})
            job_id = response.json()["id"]
            job = await queue_manager.get_job(job_id)
            # Age jobs differently and ensure timezone awareness
            assert job is not None
            job.submitted_at = (job.submitted_at - timedelta(hours=i * 12)).replace(tzinfo=timezone.utc)  # 0h, 12h, 24h old
            job.last_status_change = job.submitted_at  # Update last_status_change to match
            job.update_wait_time_weight()  # Update weight based on new submitted_at
            jobs.append(job)
            
            # Update job in database with correct timezone and wait time
            async with get_session() as session:
                stmt = (
                    update(JobModel)
                    .where(JobModel.id == job.id)
                    .values(
                        submitted_at=job.submitted_at.astimezone(timezone.utc),
                        last_status_change=job.last_status_change,
                        wait_time_weight=job.wait_time_weight
                    )
                )
                await session.execute(stmt)
                await session.commit()
                
                # Verify the update
                result = await session.execute(
                    select(JobModel).where(JobModel.id == job.id)
                )
                db_job = result.scalar_one()
                assert db_job.submitted_at.tzinfo == timezone.utc
                assert db_job.wait_time_weight == job.wait_time_weight
                assert db_job.wait_time_weight > 1.0  # Ensure weight is boosted for aged jobs
        
        # Update priorities
        await client.post("/api/v1/jobs/update-priorities")
        
        # Schedule jobs - should come out in order of wait time
        scheduled_order = []
        for _ in range(3):
            response = await client.post("/api/v1/jobs/schedule")
            scheduled_order.append(response.json()["id"])
        
        # Verify oldest job was scheduled first
        assert scheduled_order[0] == jobs[2].id  # 24h old
        assert scheduled_order[1] == jobs[1].id  # 12h old
        assert scheduled_order[2] == jobs[0].id  # 0h old
