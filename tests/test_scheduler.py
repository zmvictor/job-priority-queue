import pytest
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobCreate, JobStatus
from app.core.scheduler import GlobalMLScheduler
from app.core.queue_manager import QueueManager
from app.core.tenant import ResourceQuota

@pytest.fixture
async def queue_manager():
    manager = QueueManager()
    await manager.start()
    yield manager
    await manager.stop()
    
@pytest.fixture
async def scheduler(queue_manager):
    scheduler = GlobalMLScheduler(queue_manager)
    yield scheduler
    
def create_test_job(priority: int = 50, gpu_count: int = 1, cpu_count: int = 1) -> Job:
    """Create a test job with given priority and resource requirements."""
    job_create = JobCreate(
        name=f"test-job-{priority}",
        priority=priority,
        metadata={
            "gpu_count": gpu_count,
            "cpu_count": cpu_count,
            "total_resources": gpu_count + cpu_count
        }
    )
    return Job.create(job_create)

class TestGlobalMLScheduler:
    async def test_priority_levels(self, scheduler):
        """Test priority level assignments."""
        # Create jobs with different priorities
        jobs = [
            create_test_job(priority=scheduler.PRIORITY_LEVELS["CRITICAL"]),
            create_test_job(priority=scheduler.PRIORITY_LEVELS["HIGH"]),
            create_test_job(priority=scheduler.PRIORITY_LEVELS["MEDIUM"])
        ]
        
        # Submit jobs
        for job in jobs:
            await scheduler.submit_job(job)
            
        # Verify priority order
        scheduled_jobs = []
        while (job := await scheduler.schedule()):
            scheduled_jobs.append(job)
            
        assert len(scheduled_jobs) == 3
        assert scheduled_jobs[0].priority == scheduler.PRIORITY_LEVELS["CRITICAL"]
        assert scheduled_jobs[1].priority == scheduler.PRIORITY_LEVELS["HIGH"]
        assert scheduled_jobs[2].priority == scheduler.PRIORITY_LEVELS["MEDIUM"]
        
    async def test_credit_calculation(self, scheduler):
        """Test credit calculation based on wait time and fair share."""
        # Create jobs for different tenants
        tenant1_job = create_test_job(priority=50)
        tenant1_job.metadata["tenant_id"] = "tenant1"
        tenant2_job = create_test_job(priority=50)
        tenant2_job.metadata["tenant_id"] = "tenant2"
        
        # Set different submission times
        tenant1_job.submitted_at = datetime.now(timezone.utc) - timedelta(hours=12)
        tenant2_job.submitted_at = datetime.now(timezone.utc) - timedelta(hours=24)
        
        # Calculate credits
        credit1 = scheduler.calculate_credit(tenant1_job)
        credit2 = scheduler.calculate_credit(tenant2_job)
        
        # Verify credit calculation
        assert credit2 > credit1  # Older job should have higher credit
        assert 0.0 <= credit1 <= 1.0
        assert 0.0 <= credit2 <= 1.0
        
    async def test_quota_enforcement(self, scheduler):
        """Test quota enforcement for tenants."""
        # Set quota for tenant
        tenant = "test_tenant"
        scheduler.tenant_manager.set_quota(tenant, gpu_limit=2.0, cpu_limit=4.0)
        
        # Create jobs that would exceed quota
        jobs = [
            create_test_job(priority=50, gpu_count=1, cpu_count=2),
            create_test_job(priority=50, gpu_count=1, cpu_count=2),
            create_test_job(priority=50, gpu_count=1, cpu_count=2)
        ]
        
        for job in jobs:
            job.metadata["tenant_id"] = tenant
            
        # Submit jobs
        for job in jobs:
            await scheduler.submit_job(job)
            
        # Update priorities
        await scheduler.update_priorities()
        
        # Verify third job gets lowest priority due to quota
        assert jobs[0].priority == 50
        assert jobs[1].priority == 50
        assert jobs[2].priority == scheduler.PRIORITY_LEVELS["LOWEST"]
        
    async def test_preemption(self, scheduler):
        """Test preemption of lower priority jobs."""
        # Create and submit low priority job
        low_job = create_test_job(priority=scheduler.PRIORITY_LEVELS["LOW"])
        await scheduler.submit_job(low_job)
        
        # Schedule low priority job
        scheduled_job = await scheduler.schedule()
        assert scheduled_job.id == low_job.id
        
        # Create and submit high priority job
        high_job = create_test_job(priority=scheduler.PRIORITY_LEVELS["CRITICAL"])
        await scheduler.submit_job(high_job)
        
        # Verify preemption
        preempted = await scheduler.preempt_lower_priority_jobs(high_job)
        assert len(preempted) == 1
        assert preempted[0].id == low_job.id
        assert preempted[0].preemption_count == 1
        
        # Verify high priority job gets scheduled
        scheduled_job = await scheduler.schedule()
        assert scheduled_job.id == high_job.id
        
    async def test_fair_share(self, scheduler):
        """Test fair share scheduling between tenants."""
        # Create jobs for different tenants
        tenant1_jobs = [
            create_test_job(priority=50, gpu_count=1),
            create_test_job(priority=50, gpu_count=1)
        ]
        tenant2_jobs = [
            create_test_job(priority=50, gpu_count=1),
            create_test_job(priority=50, gpu_count=1)
        ]
        
        # Set tenant IDs
        for job in tenant1_jobs:
            job.metadata["tenant_id"] = "tenant1"
        for job in tenant2_jobs:
            job.metadata["tenant_id"] = "tenant2"
            
        # Submit all jobs
        for job in tenant1_jobs + tenant2_jobs:
            await scheduler.submit_job(job)
            
        # Update priorities
        await scheduler.update_priorities()
        
        # Verify fair share between tenants
        scheduled_jobs = []
        while (job := await scheduler.schedule()):
            scheduled_jobs.append(job)
            
        # Should alternate between tenants
        tenant_order = [job.metadata["tenant_id"] for job in scheduled_jobs]
        assert tenant_order.count("tenant1") == 2
        assert tenant_order.count("tenant2") == 2
        
    async def test_data_locality(self, scheduler):
        """Test data locality aware scheduling."""
        # Create jobs with different data locations
        jobs = [
            create_test_job(priority=50),  # No location preference
            create_test_job(priority=50),  # Same location as cluster
            create_test_job(priority=50)   # Different location
        ]
        
        # Set data locations
        jobs[1].metadata["data_location"] = "us-east"
        jobs[2].metadata["data_location"] = "us-west"
        
        # Set available clusters
        for job in jobs:
            job.metadata["available_clusters"] = ["us-east", "us-west"]
            
        # Submit jobs
        for job in jobs:
            await scheduler.submit_job(job)
            
        # Schedule jobs
        scheduled_jobs = []
        while (job := await scheduler.schedule()):
            scheduled_jobs.append(job)
            
        # Verify data locality preferences
        for job in scheduled_jobs:
            if job.metadata.get("data_location") == "us-east":
                assert job.metadata["cluster"] == "us-east"
            elif job.metadata.get("data_location") == "us-west":
                assert job.metadata["cluster"] == "us-west"
