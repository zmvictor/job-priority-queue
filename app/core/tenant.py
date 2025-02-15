from typing import Dict, List, TypeAlias, TypedDict
from datetime import datetime, timedelta

class UsageRecord(TypedDict):
    """Type for resource usage record with timestamp."""
    timestamp: datetime
    gpu: float
    cpu: float
    total: float

# Type alias for resource usage tracking
ResourceUsage: TypeAlias = Dict[str, float]

class ResourceQuota:
    """Resource quota for a tenant."""
    def __init__(self, gpu_limit: float, cpu_limit: float):
        self.gpu_limit = gpu_limit
        self.cpu_limit = cpu_limit
        self.current_gpu = 0.0
        self.current_cpu = 0.0
        
    def would_exceed(self, gpu_request: int, cpu_request: int) -> bool:
        """Check if a resource request would exceed quota."""
        return (
            self.current_gpu + gpu_request > self.gpu_limit or
            self.current_cpu + cpu_request > self.cpu_limit
        )
        
    def allocate(self, gpu_count: int, cpu_count: int) -> None:
        """Allocate resources from quota."""
        if self.would_exceed(gpu_count, cpu_count):
            raise ValueError("Resource request would exceed quota")
        self.current_gpu += gpu_count
        self.current_cpu += cpu_count
        
    def release(self, gpu_count: int, cpu_count: int) -> None:
        """Release resources back to quota."""
        self.current_gpu = max(0, self.current_gpu - gpu_count)
        self.current_cpu = max(0, self.current_cpu - cpu_count)

class TenantManager:
    """Manager for tenant quotas and resource usage history."""
    def __init__(self, history_window_hours: int = 24):
        self._quotas: Dict[str, ResourceQuota] = {}
        self._usage_history: Dict[str, List[UsageRecord]] = {}
        self._history_window = timedelta(hours=history_window_hours)
        
    def set_quota(self, tenant: str, gpu_limit: float, cpu_limit: float) -> None:
        """Set or update quota for a tenant."""
        self._quotas[tenant] = ResourceQuota(gpu_limit, cpu_limit)
        
    def get_quota(self, tenant: str) -> ResourceQuota:
        """Get quota for a tenant, creating default if none exists."""
        if tenant not in self._quotas:
            # Default quota with no limits
            self._quotas[tenant] = ResourceQuota(float('inf'), float('inf'))
        return self._quotas[tenant]
        
    def update_usage(self, tenant: str, usage: ResourceUsage, timestamp: datetime) -> None:
        """Update tenant resource usage history."""
        if tenant not in self._usage_history:
            self._usage_history[tenant] = []
            
        # Add new usage record
        record: UsageRecord = {
            'timestamp': timestamp,
            'gpu': usage['gpu'],
            'cpu': usage['cpu'],
            'total': usage['total']
        }
        self._usage_history[tenant].append(record)
        
        # Remove old records outside window
        cutoff = timestamp - self._history_window
        self._usage_history[tenant] = [
            record for record in self._usage_history[tenant]
            if record['timestamp'] > cutoff
        ]
        
    def get_usage_history(self, tenant: str) -> List[UsageRecord]:
        """Get usage history for a tenant."""
        return self._usage_history.get(tenant, [])
        
    def clear_history(self) -> None:
        """Clear all usage history."""
        self._usage_history.clear()
