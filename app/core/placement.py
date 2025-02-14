from typing import Dict, List, Optional, TypeAlias
from datetime import datetime, timedelta, timezone
from app.models.job import Job, JobStatus
from app.core.state_manager import HAJobStateManager

# Type alias for placement scores
PlacementScore: TypeAlias = Dict[str, float]

class PlacementOptimizer:
    """Optimize job placement across clusters based on preemption cost and data locality."""
    
    # Weights for different placement factors (from paper)
    PREEMPTION_WEIGHT = 0.7
    LOCALITY_WEIGHT = 0.3
    
    def __init__(self, state_manager: HAJobStateManager):
        self.state_manager = state_manager
        
    async def compute_placement_score(self, job: Job, cluster: str) -> float:
        """Compute placement quality score for a job in a cluster.
        
        The score is a weighted combination of:
        1. Preemption cost (lower is better)
        2. Data locality score (higher is better)
        """
        running_jobs = await self.state_manager.get_running_jobs()
        preemption_cost = await self._calculate_preemption_cost(job, running_jobs, cluster)
        locality_score = self._calculate_locality_score(job, cluster)
        
        # Normalize preemption cost to [0, 1] range where 1 is best (no preemption)
        if running_jobs:
            normalized_cost = 1.0 - min(1.0, preemption_cost / len(running_jobs))
        else:
            normalized_cost = 1.0  # No preemption needed
            
        return (
            self.PREEMPTION_WEIGHT * normalized_cost +
            self.LOCALITY_WEIGHT * locality_score
        )
        
    async def _calculate_preemption_cost(self, job: Job, running_jobs: List[Job], cluster: str) -> float:
        """Calculate preemption cost for placing job in cluster.
        
        Cost factors (from paper):
        1. Number of jobs that need preemption
        2. Priority of jobs being preempted
        3. Runtime of jobs being preempted
        """
        if not running_jobs:
            return 0.0  # No preemption needed
            
        # Filter jobs in target cluster
        cluster_jobs = [j for j in running_jobs if j.metadata.get("cluster") == cluster]
        
        # Calculate available resources
        available_gpu = self._get_cluster_gpu_capacity(cluster)
        available_cpu = self._get_cluster_cpu_capacity(cluster)
        
        for j in cluster_jobs:
            available_gpu -= float(j.metadata.get("gpu_count", 0))
            available_cpu -= float(j.metadata.get("cpu_count", 1))
            
        # Check if we need preemption
        job_gpu = float(job.metadata.get("gpu_count", 0))
        job_cpu = float(job.metadata.get("cpu_count", 1))
        
        if available_gpu >= job_gpu and available_cpu >= job_cpu:
            return 0.0  # No preemption needed
            
        # Calculate preemption cost
        cost = 0.0
        needed_gpu = max(0, job_gpu - available_gpu)
        needed_cpu = max(0, job_cpu - available_cpu)
        
        # Sort jobs by priority (lowest first) and runtime
        cluster_jobs.sort(key=lambda j: (
            j.priority,
            (datetime.now(timezone.utc) - j.submitted_at).total_seconds()
        ))
        
        # Calculate cost based on jobs we need to preempt
        current_gpu = 0
        current_cpu = 0
        for j in cluster_jobs:
            if current_gpu >= needed_gpu and current_cpu >= needed_cpu:
                break
                
            j_gpu = float(j.metadata.get("gpu_count", 0))
            j_cpu = float(j.metadata.get("cpu_count", 1))
            
            # Add to preemption cost:
            # 1. Base cost of 1.0 per job
            # 2. Priority factor (higher priority = higher cost)
            # 3. Runtime factor (longer runtime = higher cost)
            runtime_hours = (datetime.now(timezone.utc) - j.submitted_at).total_seconds() / 3600
            priority_factor = j.priority / 100.0  # Normalize to [0,1]
            runtime_factor = min(1.0, runtime_hours / 24.0)  # Cap at 1 day
            cost += 1.0 + priority_factor + runtime_factor
            
            current_gpu += j_gpu
            current_cpu += j_cpu
            
        return cost
        
    def _calculate_locality_score(self, job: Job, cluster: str) -> float:
        """Calculate data locality score for placing job in cluster.
        
        Score is based on:
        1. Data transfer cost between job's data location and cluster
        2. Network bandwidth and latency between locations
        """
        # Get job's data location from metadata
        data_location = job.metadata.get("data_location", "")
        if not data_location or data_location == cluster:
            return 1.0  # Perfect locality
            
        # Calculate locality score based on network topology
        # For now, use a simple distance-based score
        # TODO: Implement more sophisticated locality scoring based on:
        # 1. Network topology
        # 2. Bandwidth measurements
        # 3. Historical transfer times
        return self._get_network_distance_score(data_location, cluster)
        
    def _get_network_distance_score(self, source: str, target: str) -> float:
        """Calculate network distance score between locations."""
        # TODO: Implement actual network topology scoring
        # For now, return:
        # 1.0 for same location
        # 0.8 for same region
        # 0.3 for different regions
        if source == target:
            return 1.0
        if source.split("-")[0] == target.split("-")[0]:  # Same region
            return 0.8
        return 0.3
        
    def _get_cluster_gpu_capacity(self, cluster: str) -> float:
        """Get total GPU capacity for cluster."""
        # TODO: Implement cluster capacity tracking
        return float("inf")
        
    def _get_cluster_cpu_capacity(self, cluster: str) -> float:
        """Get total CPU capacity for cluster."""
        # TODO: Implement cluster capacity tracking
        return float("inf")
