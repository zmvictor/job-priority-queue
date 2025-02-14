import pytest
import json
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobCreate, JobStatus
from app.models.database import get_session, JobModel, JobStatusEnum
from app.core.placement import PlacementOptimizer
from app.core.state_manager import HAJobStateManager

@pytest.fixture
async def state_manager():
    manager = HAJobStateManager()
    await manager.start()
    yield manager
    await manager.stop()
    
@pytest.fixture
def placement_optimizer(state_manager):
    return PlacementOptimizer(state_manager)
    
def create_test_job(
    priority: int = 50,
    gpu_count: int = 1,
    cpu_count: int = 1,
    data_location: str = ""
) -> Job:
    """Create a test job with given priority and resource requirements."""
    job_create = JobCreate(
        name=f"test-job-{priority}",
        priority=priority,
        metadata={
            "gpu_count": gpu_count,
            "cpu_count": cpu_count,
            "total_resources": gpu_count + cpu_count,
            "data_location": data_location
        }
    )
    return Job.create(job_create)

class TestPlacementOptimizer:
    async def test_placement_score_calculation(self, placement_optimizer):
        """Test placement score calculation."""
        # Create job with data locality
        job = create_test_job(data_location="us-east")
        
        # Calculate scores for different clusters
        same_loc_score = await placement_optimizer.compute_placement_score(job, "us-east")
        same_region_score = await placement_optimizer.compute_placement_score(job, "us-east-2")
        diff_region_score = await placement_optimizer.compute_placement_score(job, "us-west")
        
        # Verify locality affects scores
        assert same_loc_score > same_region_score > diff_region_score
        assert 0.0 <= diff_region_score <= 0.3  # Different region score
        assert 0.7 <= same_region_score <= 0.8  # Same region score
        assert 0.9 <= same_loc_score <= 1.0     # Same location score
        
    async def test_preemption_cost(self, placement_optimizer, state_manager):
        """Test preemption cost calculation."""
        # Create running jobs in cluster
        running_jobs = [
            create_test_job(priority=30, gpu_count=1),  # Low priority
            create_test_job(priority=70, gpu_count=1)   # High priority
        ]
        
        for job in running_jobs:
            # Create new job with cluster metadata
            metadata = dict(job.metadata)
            metadata["cluster"] = "test-cluster"
            job_create = JobCreate(
                name=job.name,
                priority=job.priority,
                metadata=metadata
            )
            new_job = Job.create(job_create)
            await state_manager.transition_to_running(new_job)
            
        # Calculate cost for new job that requires preemption
        new_job = create_test_job(priority=50, gpu_count=2)  # Requires preempting
        cost = await placement_optimizer._calculate_preemption_cost(
            new_job, running_jobs, "test-cluster"
        )
        
        # Should only need to preempt low priority job
        assert cost > 0.0  # Has preemption cost
        assert cost < 2.0  # But not both jobs
        
    async def test_data_locality_score(self, placement_optimizer):
        """Test data locality scoring."""
        # Create jobs with different data locations
        jobs = [
            create_test_job(priority=50, data_location="us-east"),
            create_test_job(priority=50, data_location="us-west"),
            create_test_job(priority=50)  # No location preference
        ]
        
        # Test locality scores
        assert placement_optimizer._get_network_distance_score("us-east", "us-east") == 1.0
        assert placement_optimizer._get_network_distance_score("us-east", "us-east-2") == 0.8
        assert placement_optimizer._get_network_distance_score("us-east", "us-west") == 0.3
        assert placement_optimizer._get_network_distance_score("us-west", "us-west") == 1.0
        assert placement_optimizer._get_network_distance_score("", "anywhere") == 1.0
        
        # Test score ordering
        assert (
            placement_optimizer._get_network_distance_score("us-east", "us-east") >
            placement_optimizer._get_network_distance_score("us-east", "us-east-2") >
            placement_optimizer._get_network_distance_score("us-east", "us-west")
        )
        
    async def test_resource_availability(self, placement_optimizer, state_manager):
        """Test resource availability affects placement."""
        # Create cluster with running jobs
        cluster = "test-cluster"
        running_jobs = [
            create_test_job(gpu_count=2, cpu_count=2),
            create_test_job(gpu_count=2, cpu_count=2)
        ]
        
        for job in running_jobs:
            metadata = dict(job.metadata)
            metadata["cluster"] = cluster
            job_create = JobCreate(
                name=job.name,
                priority=job.priority,
                metadata=metadata
            )
            new_job = Job.create(job_create)
            await state_manager.transition_to_running(new_job)
            
        # Try to place job that fits
        small_job = create_test_job(gpu_count=1, cpu_count=1)
        small_score = await placement_optimizer.compute_placement_score(small_job, cluster)
        
        # Try to place job that doesn't fit
        large_job = create_test_job(gpu_count=3, cpu_count=3)
        large_score = await placement_optimizer.compute_placement_score(large_job, cluster)
        
        # Job that fits should score higher
        assert small_score > large_score
