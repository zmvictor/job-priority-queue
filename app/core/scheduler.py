from typing import Optional, List, Dict
from datetime import datetime, timedelta
from app.models.job import Job, JobStatus, JobCreate
from app.core.queue_manager import QueueManager
from app.core.tenant import TenantManager, ResourceUsage, UsageRecord
from app.core.placement import PlacementOptimizer

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
        self.tenant_manager = TenantManager()
        self.placement_optimizer = PlacementOptimizer(self.state_manager)
        self._tenant_resources: Dict[str, ResourceUsage] = {}
    
    def calculate_credit(self, workload: Job) -> float:
        """Calculate workload credit based on wait time and tenant resource usage."""
        age_credit = min(workload.calculate_wait_time() / self.AGE_CAP, 1.0)
        
        tenant = self._get_tenant(workload)
        all_tenant_usage = self._get_window_avg_usage_all_tenants()
        if all_tenant_usage == 0:
            fair_share = 1.0
        else:
            fair_share = 1.0 - min(1.0, self._get_window_avg_usage(tenant) / all_tenant_usage)
        
        return (
            self.WORKLOAD_AGE_WEIGHT * age_credit + 
            self.FAIR_SHARE_WEIGHT * fair_share
        )
    
    async def update_priorities(self) -> None:
        """Update priorities and credits for all pending workloads."""
        # Get jobs from queue manager's priority queue
        jobs = []
        while (job := self.queue_manager.queue.dequeue()):
            jobs.append(job)
            
        if not jobs:
            return
            
        tenant_resources = await self._calculate_tenant_resources()
        
        for job in sorted(jobs, key=lambda j: j.submitted_at):
            tenant = self._get_tenant(job)
            if tenant not in tenant_resources:
                tenant_resources[tenant] = {"gpu": 0.0, "cpu": 0.0, "total": 0.0}
                
            if self._would_exceed_quota(tenant, job, tenant_resources):
                object.__setattr__(job, 'priority', self.PRIORITY_LEVELS["LOWEST"])
            else:
                resources = tenant_resources[tenant]
                job_resources = self._get_job_resources(job)
                resources["gpu"] += job_resources["gpu"]
                resources["cpu"] += job_resources["cpu"]
                resources["total"] += job_resources["total"]
                job.credit = self.calculate_credit(job)
                
        # Re-enqueue jobs with updated priorities
        for job in jobs:
            self.queue_manager.queue.enqueue(job)
    
    async def schedule(self) -> Optional[Job]:
        """Schedule the next job based on global policy."""
        # Update priorities and credits
        await self.update_priorities()
        
        # Get next job from queue
        job = self.queue_manager.queue.dequeue()
        if not job:
            return None
            
        # Find best cluster for job placement
        best_cluster = None
        best_score = -1.0
        
        # Get available clusters from job metadata
        available_clusters = job.metadata.get("available_clusters", [])
        if not available_clusters:
            # If data location is specified, use that region
            data_location = job.metadata.get("data_location", "")
            if data_location:
                available_clusters = [data_location]
            else:
                available_clusters = ["default"]  # Fallback to default cluster
            
        # Score each cluster
        for cluster in available_clusters:
            score = await self.placement_optimizer.compute_placement_score(job, cluster)
            if score > best_score:
                best_score = score
                best_cluster = cluster
                
        if best_cluster:
            # Create new job with updated metadata
            metadata = dict(job.metadata)
            metadata["cluster"] = best_cluster
            metadata["tenant_id"] = self._get_tenant(job)  # Ensure tenant ID is set
            job_create = JobCreate(
                name=job.name,
                priority=job.priority,
                metadata=metadata
            )
            new_job = Job.create(job_create)
            # Preserve original job ID and other fields
            object.__setattr__(new_job, 'id', job.id)
            object.__setattr__(new_job, 'submitted_at', job.submitted_at)
            object.__setattr__(new_job, 'last_status_change', job.last_status_change)
            object.__setattr__(new_job, 'preemption_count', job.preemption_count)
            object.__setattr__(new_job, 'wait_time_weight', job.wait_time_weight)
            object.__setattr__(new_job, 'credit', job.credit)
            job = new_job
            
            # Try to transition job to running
            try:
                updated_job = await self.state_manager.transition_to_running(job)
                if updated_job and updated_job.status == JobStatus.RUNNING:
                    return updated_job
            except Exception as e:
                # If transition failed, re-enqueue job
                self.queue_manager.queue.enqueue(job)
                raise e
                
        # Re-enqueue job if scheduling failed
        self.queue_manager.queue.enqueue(job)
        return None
    
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
        # Handle nested metadata structure
        metadata = job.metadata
        if isinstance(metadata, dict):
            # First check top-level metadata
            if "tenant_id" in metadata:
                return metadata["tenant_id"]
            # Then check nested metadata
            if "metadata" in metadata and isinstance(metadata["metadata"], dict):
                if "tenant_id" in metadata["metadata"]:
                    tenant_id = metadata["metadata"]["tenant_id"]
                    # Move tenant_id to top level
                    metadata = dict(metadata)
                    metadata["tenant_id"] = tenant_id
                    object.__setattr__(job, 'metadata', metadata)
                    return tenant_id
        # Set default tenant_id
        metadata = dict(metadata)
        metadata["tenant_id"] = "default"
        object.__setattr__(job, 'metadata', metadata)
        return "default"
    
    def _get_window_avg_usage(self, tenant: str) -> float:
        """Get average resource usage for a tenant over the history window."""
        history = self.tenant_manager.get_usage_history(tenant)
        if not history:
            return 0.0
        return sum(usage["total"] for usage in history) / len(history)
    
    def _get_window_avg_usage_all_tenants(self) -> float:
        """Get average resource usage across all tenants."""
        total_usage = 0.0
        total_records = 0
        for tenant in self._tenant_resources:
            history = self.tenant_manager.get_usage_history(tenant)
            if history:
                total_usage += sum(usage["total"] for usage in history)
                total_records += len(history)
        if total_records == 0:
            return 0.0  # No usage yet
        return total_usage / total_records
    
    async def _calculate_tenant_resources(self) -> Dict[str, ResourceUsage]:
        """Calculate current resource usage per tenant."""
        # Initialize resources for all tenants with quotas
        resources = {}
        for tenant in self.tenant_manager._quotas:
            resources[tenant] = {"gpu": 0.0, "cpu": 0.0, "total": 0.0}
        
        # Add default tenant if not present
        if "default" not in resources:
            resources["default"] = {"gpu": 0.0, "cpu": 0.0, "total": 0.0}
            
        running_jobs = await self.state_manager.get_running_jobs()
        for job in running_jobs:
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
        quota = self.tenant_manager.get_quota(tenant)
        if resource == "gpu":
            return quota.gpu_limit
        elif resource == "cpu":
            return quota.cpu_limit
        return float("inf")
