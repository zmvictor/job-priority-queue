from typing import Optional, List, Dict, TypeAlias
from datetime import datetime, timedelta
from app.models.job import Job, JobStatus
from app.core.queue_manager import QueueManager

# Type alias for resource usage tracking
ResourceUsage: TypeAlias = Dict[str, float]

class GlobalMLScheduler:
    """Global ML Scheduler (GMS) implementation based on the paper.
    
    The GMS is responsible for:
    1. Global job queue management
    2. Priority-based scheduling with 7 priority levels
    3. Dynamic priority adjustments based on quota usage
    4. Credit calculation using workload age and fair share
    """
    
    # Constants for priority levels (distribution from paper)
    PRIORITY_LEVELS = {
        "CRITICAL": 100,  # 3%
        "HIGH": 80,      # 20%
        "MEDIUM_HIGH": 60,  # 16%
        "MEDIUM": 40,    # 54%
        "MEDIUM_LOW": 20,  # 0.2%
        "LOW": 10,       # 0.5%
        "LOWEST": 0      # 0.02% + 6% unspecified
    }
    
    # Constants for credit calculation
    AGE_CAP = 24.0  # Maximum age credit in hours
    WORKLOAD_AGE_WEIGHT = 0.7
    FAIR_SHARE_WEIGHT = 0.3
    
    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        self.state_manager = queue_manager.state_manager
        self._tenant_resources: Dict[str, ResourceUsage] = {}
        self._usage_history: Dict[str, List[ResourceUsage]] = {}
        self._history_window = 24  # Hours of history to maintain
    
    def calculate_credit(self, workload: Job) -> float:
        """Calculate workload credit based on wait time and tenant resource usage."""
        age_credit = min(workload.calculate_wait_time(), self.AGE_CAP)
        
        tenant = self._get_tenant(workload)
        fair_share = 1.0 - (
            self._get_window_avg_usage(tenant) / 
            self._get_window_avg_usage_all_tenants()
        )
        
        return (
            self.WORKLOAD_AGE_WEIGHT * age_credit + 
            self.FAIR_SHARE_WEIGHT * fair_share
        )
    
    async def update_priorities(self) -> None:
        """Update priorities and credits for all pending workloads."""
        pending_jobs = await self.queue_manager.get_pending_jobs()
        tenant_resources = self._calculate_tenant_resources()
        
        for job in sorted(pending_jobs, key=lambda j: j.submitted_at):
            tenant = self._get_tenant(job)
            if self._would_exceed_quota(tenant, job, tenant_resources):
                job.priority = self.PRIORITY_LEVELS["LOWEST"]
            else:
                tenant_resources[tenant] += self._get_job_resources(job)
                job.credit = self.calculate_credit(job)
    
    async def schedule(self) -> Optional[Job]:
        """Schedule the next job based on global policy."""
        # Update priorities and credits
        await self.update_priorities()
        
        # Get all pending jobs
        pending_jobs = await self.queue_manager.get_pending_jobs()
        if not pending_jobs:
            return None
            
        # Sort by priority and credit
        pending_jobs.sort(key=lambda j: (-j.priority, -j.credit))
        
        # Try to schedule highest priority job
        return await self.queue_manager.schedule_next_job()
    
    async def preempt_lower_priority_jobs(self, new_job: Job) -> List[Job]:
        """Preempt lower priority running jobs if needed."""
        preempted_jobs = []
        running_jobs = await self.queue_manager.get_running_jobs()
        
        # Sort running jobs by priority and credit (lowest first)
        running_jobs.sort(key=lambda j: (j.priority, j.credit))
        
        for job in running_jobs:
            if job.priority < new_job.priority:
                try:
                    preempted_job = await self.queue_manager.preempt_job(job.id)
                    if preempted_job:
                        preempted_jobs.append(preempted_job)
                except Exception as e:
                    print(f"Error preempting job {job.id}: {str(e)}")
                    
        return preempted_jobs
    
    async def submit_job(self, job: Job) -> None:
        """Submit a new job and handle preemption if needed."""
        # First submit the job to queue
        await self.queue_manager.submit_job(job)
        
        # Update priorities to get proper ordering
        await self.update_priorities()
        
        # Check if we need to preempt lower priority jobs
        running_jobs = await self.queue_manager.get_running_jobs()
        if running_jobs:
            lowest_priority = min(j.priority for j in running_jobs)
            if job.priority > lowest_priority:
                await self.preempt_lower_priority_jobs(job)
                await self.schedule()
    
    def _get_tenant(self, job: Job) -> str:
        """Get tenant ID from job metadata."""
        return job.metadata.get("tenant_id", "default")
    
    def _get_window_avg_usage(self, tenant: str) -> float:
        """Get average resource usage for a tenant over the history window."""
        history = self._usage_history.get(tenant, [])
        if not history:
            return 0.0
        return sum(usage["total"] for usage in history) / len(history)
    
    def _get_window_avg_usage_all_tenants(self) -> float:
        """Get average resource usage across all tenants."""
        all_usage = [
            usage
            for tenant in self._usage_history.values()
            for usage in tenant
        ]
        if not all_usage:
            return 1.0  # Avoid division by zero
        return sum(usage["total"] for usage in all_usage) / len(all_usage)
    
    def _calculate_tenant_resources(self) -> Dict[str, ResourceUsage]:
        """Calculate current resource usage per tenant."""
        resources = {}
        for job in self.state_manager.get_running_jobs():
            tenant = self._get_tenant(job)
            if tenant not in resources:
                resources[tenant] = {"gpu": 0.0, "cpu": 0.0, "total": 0.0}
            job_resources = self._get_job_resources(job)
            resources[tenant]["gpu"] += job_resources["gpu"]
            resources[tenant]["cpu"] += job_resources["cpu"]
            resources[tenant]["total"] += job_resources["total"]
        return resources
    
    def _get_job_resources(self, job: Job) -> ResourceUsage:
        """Get resource requirements for a job."""
        return {
            "gpu": float(job.metadata.get("gpu_count", 0)),
            "cpu": float(job.metadata.get("cpu_count", 1)),
            "total": float(job.metadata.get("total_resources", 1))
        }
    
    def _would_exceed_quota(self, tenant: str, job: Job, current_resources: Dict[str, ResourceUsage]) -> bool:
        """Check if adding job would exceed tenant quota."""
        if tenant not in current_resources:
            return False
        
        job_resources = self._get_job_resources(job)
        tenant_resources = current_resources[tenant]
        
        # Check against quota limits
        return (
            tenant_resources["gpu"] + job_resources["gpu"] > self._get_tenant_quota(tenant, "gpu") or
            tenant_resources["cpu"] + job_resources["cpu"] > self._get_tenant_quota(tenant, "cpu")
        )
    
    def _get_tenant_quota(self, tenant: str, resource: str) -> float:
        """Get quota limit for a tenant and resource type."""
        # TODO: Implement quota management in next step
        return float("inf")  # No quota limits for now
