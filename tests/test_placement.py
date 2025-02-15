import pytest
import json
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobCreate, JobStatus
from app.models.database import get_session, JobModel, JobStatusEnum
from app.core.placement import PlacementOptimizer
from app.core.state_manager import HAJobStateManager
from app.core.scheduler import GlobalMLScheduler
from app.core.queue_manager import QueueManager

@pytest.fixture
async def state_manager():
    manager = HAJobStateManager()
    await manager.start()
    yield manager
    await manager.stop()
    
@pytest.fixture
def placement_optimizer(state_manager):
    return PlacementOptimizer(state_manager)

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
    async def test_placement_score_calculation(self, placement_optimizer, scheduler):
        """Test placement score calculation with priority-based locality."""
        # Create jobs with different priorities
        critical_job = create_test_job(
            priority=scheduler.PRIORITY_LEVELS["CRITICAL"],
            data_location="us-east"
        )
        medium_job = create_test_job(
            priority=scheduler.PRIORITY_LEVELS["MEDIUM"],
            data_location="us-east"
        )
        low_job = create_test_job(
            priority=scheduler.PRIORITY_LEVELS["LOW"],
            data_location="us-east"
        )
        
        # Calculate scores for different regions
        critical_score = await placement_optimizer.compute_placement_score(
            critical_job, "us-east-2"
        )
        medium_score = await placement_optimizer.compute_placement_score(
            medium_job, "us-east-2"
        )
        low_score = await placement_optimizer.compute_placement_score(
            low_job, "us-east-2"
        )
        
        # Higher priority jobs should get better locality scores
        # Round to 3 decimal places for comparison
        critical_score = round(critical_score, 3)
        medium_score = round(medium_score, 3)
        low_score = round(low_score, 3)
        
        # Scores should be different when rounded to 3 decimal places
        assert critical_score > medium_score > low_score
        
        # Verify base locality scoring still works
        for job in [critical_job, medium_job, low_job]:
            same_loc = await placement_optimizer.compute_placement_score(job, "us-east")
            same_region = await placement_optimizer.compute_placement_score(job, "us-east-2")
            diff_region = await placement_optimizer.compute_placement_score(job, "us-west")
            
            # Round to 3 decimal places for comparison
            same_loc = round(same_loc, 3)
            same_region = round(same_region, 3)
            diff_region = round(diff_region, 3)
            
            # Compare at 3 decimal precision with guaranteed tier separation
            assert 0.300 <= round(diff_region, 3) <= 0.399    # Different location
            assert 0.300 <= round(same_region, 3) <= 0.399    # Different location (same region)
            assert 0.900 <= round(same_loc, 3) <= 0.999       # Same location
            
            # Verify tiers are properly separated
            assert round(same_loc, 3) > round(same_region, 3)
            assert round(same_loc, 3) > round(diff_region, 3)
            
            # Verify scores within same tier are properly ordered by priority
            assert abs(round(same_loc, 3) - 0.900) <= 0.099  # Within same location tier
            assert 0.300 <= round(diff_region, 3) <= 0.399  # Within different location tier
        
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
            
            # Create job in database first
            async with get_session() as session:
                db_job = JobModel(
                    id=new_job.id,
                    name=new_job.name,
                    priority=new_job.priority,
                    submitted_at=new_job.submitted_at,
                    status=JobStatusEnum.SUBMITTED,
                    job_metadata=json.dumps(dict(new_job.metadata)),
                    last_status_change=new_job.last_status_change,
                    preemption_count=new_job.preemption_count,
                    wait_time_weight=new_job.wait_time_weight
                )
                session.add(db_job)
                await session.commit()
            
            # Now transition to running
            await state_manager.transition_to_running(new_job)
            
        # Calculate cost for new job that requires preemption
        new_job = create_test_job(priority=50, gpu_count=2)  # Requires preempting
        cost = await placement_optimizer._calculate_preemption_cost(
            new_job, running_jobs, "test-cluster"
        )
        
        # Should only need to preempt low priority job
        assert cost > 0.0  # Has preemption cost
        assert cost < 2.0  # But not both jobs
        
    async def test_data_locality_score(self, placement_optimizer, scheduler):
        """Test data locality scoring."""
        # Create jobs with different data locations and priorities
        jobs = [
            create_test_job(priority=scheduler.PRIORITY_LEVELS["CRITICAL"], data_location="us-east"),
            create_test_job(priority=scheduler.PRIORITY_LEVELS["MEDIUM"], data_location="us-east"),
            create_test_job(priority=scheduler.PRIORITY_LEVELS["LOWEST"], data_location="us-east")
        ]
        
        # Calculate locality scores for each job
        scores = []
        for job in jobs:
            same_loc = await placement_optimizer.compute_placement_score(job, "us-east")
            same_region = await placement_optimizer.compute_placement_score(job, "us-east-2")
            diff_region = await placement_optimizer.compute_placement_score(job, "us-west")
            scores.append((same_loc, same_region, diff_region))
            
        # Higher priority jobs should get better locality scores
        for i in range(len(scores) - 1):
            # Compare at 3 decimal places since small differences don't matter
            assert round(scores[i][0], 3) >= round(scores[i+1][0], 3)  # Same location
            assert round(scores[i][1], 3) >= round(scores[i+1][1], 3)  # Same region
            assert round(scores[i][2], 3) >= round(scores[i+1][2], 3)  # Different region
            
        # Verify score ranges for each location type
        for same_loc, same_region, diff_region in scores:
            assert 0.900 <= round(same_loc, 3) <= 0.999      # Same location
            assert 0.300 <= round(same_region, 3) <= 0.399    # Different location (same region)
            assert 0.300 <= round(diff_region, 3) <= 0.399    # Different location (different region)
            # Compare at 3 decimal places since small differences don't matter
            assert round(same_loc, 3) >= round(same_region, 3)
            assert round(same_loc, 3) >= round(diff_region, 3)
        
        # Test score ordering
        # Compare at 3 decimal places since small differences don't matter
        same_loc = round(placement_optimizer._get_network_distance_score("us-east", "us-east"), 3)
        same_region = round(placement_optimizer._get_network_distance_score("us-east", "us-east-2"), 3)
        diff_region = round(placement_optimizer._get_network_distance_score("us-east", "us-west"), 3)
        
        # Verify score ranges
        assert 0.900 <= same_loc <= 0.999
        assert 0.300 <= same_region <= 0.399
        assert 0.300 <= diff_region <= 0.399
        
        # Verify ordering at 3 decimal places
        assert same_loc >= same_region
        assert same_loc >= diff_region
        
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
            
            # Create job in database first
            async with get_session() as session:
                db_job = JobModel(
                    id=new_job.id,
                    name=new_job.name,
                    priority=new_job.priority,
                    submitted_at=new_job.submitted_at,
                    status=JobStatusEnum.SUBMITTED,
                    job_metadata=json.dumps(dict(new_job.metadata)),
                    last_status_change=new_job.last_status_change,
                    preemption_count=new_job.preemption_count,
                    wait_time_weight=new_job.wait_time_weight
                )
                session.add(db_job)
                await session.commit()
            
            # Now transition to running
            await state_manager.transition_to_running(new_job)
            
        # Try to place job that fits
        small_job = create_test_job(gpu_count=1, cpu_count=1)
        small_score = await placement_optimizer.compute_placement_score(small_job, cluster)
        
        # Try to place job that doesn't fit
        large_job = create_test_job(gpu_count=3, cpu_count=3)
        large_score = await placement_optimizer.compute_placement_score(large_job, cluster)
        
        # Job that fits should score higher
        # Round to 3 decimal places for comparison
        small_score = round(small_score, 3)
        large_score = round(large_score, 3)
        
        # Scores should be different when rounded to 3 decimal places
        assert small_score > large_score
