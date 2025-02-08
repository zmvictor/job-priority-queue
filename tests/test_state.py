import pytest
import os
import json
from datetime import datetime, timedelta, UTC
from app.models.job import Job, JobStatus
from app.core.state import JobStateManager

def create_test_job(job_id: str = "test-1", priority: int = 50) -> Job:
    now = datetime.now(UTC)
    return Job(
        id=job_id,
        name=f"test-job-{job_id}",
        priority=priority,
        submitted_at=now,
        status=JobStatus.SUBMITTED,
        metadata={},
        last_status_change=now,
    )

class TestJobStateManager:
    @pytest.fixture
    def state_file(self, tmp_path):
        return str(tmp_path / "test_state.json")
    
    @pytest.fixture
    def state_manager(self, state_file):
        manager = JobStateManager(state_file=state_file)
        yield manager
        # Cleanup
        if os.path.exists(state_file):
            os.remove(state_file)
    
    def test_state_transitions(self, state_manager):
        job = create_test_job()
        
        # Test transition to running
        state_manager.transition_to_running(job)
        assert job.status == JobStatus.RUNNING
        assert state_manager.get_job_state(job.id) == job
        
        # Test preemption
        state_manager.preempt_job(job)
        assert job.status == JobStatus.PREEMPTED
        assert job.preemption_count == 1
        assert state_manager.get_job_state(job.id) is None
        
        # Test completion
        state_manager.transition_to_running(job)
        state_manager.complete_job(job)
        assert job.status == JobStatus.COMPLETED
        assert state_manager.get_job_state(job.id) is None
    
    def test_high_availability_persistence(self, state_manager, state_file):
        job = create_test_job()
        state_manager.transition_to_running(job)
        
        # Verify state was persisted
        assert os.path.exists(state_file)
        with open(state_file, 'r') as f:
            state = json.load(f)
            assert job.id in state["running_jobs"]
        
        # Create new manager and verify state is loaded
        new_manager = JobStateManager(state_file=state_file)
        loaded_job = new_manager.get_job_state(job.id)
        assert loaded_job is not None
        assert loaded_job.id == job.id
        assert loaded_job.status == JobStatus.RUNNING
    
    def test_concurrent_running_jobs(self, state_manager):
        jobs = [create_test_job(f"job-{i}") for i in range(3)]
        for job in jobs:
            state_manager.transition_to_running(job)
        
        running_jobs = state_manager.get_running_jobs()
        assert len(running_jobs) == 3
        assert all(job.status == JobStatus.RUNNING for job in running_jobs)
    
    def test_preemption_tracking(self, state_manager):
        job = create_test_job()
        
        # Multiple preemptions
        state_manager.transition_to_running(job)
        state_manager.preempt_job(job)
        assert job.preemption_count == 1
        
        state_manager.transition_to_running(job)
        state_manager.preempt_job(job)
        assert job.preemption_count == 2
    
    def test_state_recovery_after_corruption(self, state_manager, state_file):
        # Write invalid JSON
        with open(state_file, 'w') as f:
            f.write("invalid json")
        
        # New manager should handle corruption gracefully
        new_manager = JobStateManager(state_file=state_file)
        assert len(new_manager.get_running_jobs()) == 0
    
    def test_atomic_state_updates(self, state_manager, state_file):
        job = create_test_job()
        state_manager.transition_to_running(job)
        
        # Verify temp file was cleaned up
        temp_file = f"{state_file}.tmp"
        assert not os.path.exists(temp_file)
        
        # Verify main state file exists and is valid
        assert os.path.exists(state_file)
        with open(state_file, 'r') as f:
            state = json.load(f)
            assert isinstance(state, dict)
            assert "running_jobs" in state
