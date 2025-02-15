import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobStatus
from app.models.database import get_session, JobModel
from app.core.queue_manager import queue_manager

pytestmark = pytest.mark.asyncio

class TestJobLifecycle:
    async def test_full_job_lifecycle(self, test_client):
        """Test the complete lifecycle of a job through the API."""
        # Submit job
        job_data = {"name": "training-job", "priority": 50}
        response = await test_client.post("/api/v1/jobs", json=job_data)
        assert response.status_code == 200
        job_id = response.json()["id"]
        
        # Verify job status
        response = await test_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json()["status"] == JobStatus.SUBMITTED.value
        
        # Schedule job
        response = await test_client.post("/api/v1/jobs/schedule")
        assert response.status_code == 200
        scheduled_job = response.json()
        assert scheduled_job["id"] == job_id
        assert scheduled_job["status"] == JobStatus.RUNNING.value
        
        # Verify running jobs list
        response = await test_client.get("/api/v1/jobs")
        assert response.status_code == 200
        running_jobs = response.json()
        assert len(running_jobs) == 1
        assert running_jobs[0]["id"] == job_id
