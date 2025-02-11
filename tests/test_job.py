import pytest
from datetime import datetime, timedelta, UTC
from app.models.job import Job, JobCreate, JobStatus

@pytest.fixture
def job_create():
    return JobCreate(
        name="test_job",
        priority=50,
        metadata={"batch_size": 32}
    )

@pytest.fixture
def job(job_create):
    return Job.create(job_create)

def test_job_creation(job_create, job):
    assert job.name == "test_job"
    assert job.priority == 50
    assert job.status == JobStatus.SUBMITTED
    assert job.metadata == {"batch_size": 32}
    assert job.preemption_count == 0
    assert job.wait_time_weight == 1.0

def test_job_status_update(job):
    job.update_status(JobStatus.RUNNING)
    assert job.status == JobStatus.RUNNING
    assert (datetime.now(UTC) - job.last_status_change).total_seconds() < 1

def test_job_preemption(job):
    initial_count = job.preemption_count
    job.increment_preemption()
    assert job.preemption_count == initial_count + 1
    assert job.status == JobStatus.PREEMPTED

def test_wait_time_calculation(job):
    # Set submitted_at to 2 hours ago
    job.submitted_at = datetime.now(UTC) - timedelta(hours=2)
    wait_time = job.calculate_wait_time()
    assert 1.9 <= wait_time <= 2.1  # Allow small time difference

def test_job_priority_validation():
    """Test job priority validation."""
    # Test valid priorities
    for priority in [0, 50, 100]:
        job = Job.create(JobCreate(name="test", priority=priority))
        assert job.priority == priority

    # Test invalid priorities
    with pytest.raises(ValueError):
        Job.create(JobCreate(name="test", priority=-1))
    with pytest.raises(ValueError):
        Job.create(JobCreate(name="test", priority=101))

def test_job_metadata_handling(job):
    """Test job metadata handling."""
    # Test metadata access
    metadata = job.get_metadata()
    assert metadata["batch_size"] == 32
    
    # Test metadata immutability
    original_metadata = job.get_metadata()
    with pytest.raises(TypeError):  # frozen=True makes the field immutable
        job.metadata["new_key"] = "new_value"
    assert job.get_metadata() == original_metadata

def test_job_state_transitions(job):
    """Test valid job state transitions."""
    valid_transitions = [
        (JobStatus.SUBMITTED, JobStatus.PENDING),
        (JobStatus.PENDING, JobStatus.SCHEDULED),
        (JobStatus.SCHEDULED, JobStatus.RUNNING),
        (JobStatus.RUNNING, JobStatus.COMPLETED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.PREEMPTED)
    ]
    
    for from_state, to_state in valid_transitions:
        job.status = from_state
        job.update_status(to_state)
        assert job.status == to_state
