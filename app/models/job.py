from datetime import datetime, UTC
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
import uuid

class JobStatus(str, Enum):
    SUBMITTED = "submitted"
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PREEMPTED = "preempted"

class JobCreate(BaseModel):
    name: str
    priority: int = Field(ge=0, le=100, description="Job priority (0-100)")
    metadata: Dict[str, Any] = Field(default_factory=dict)

class Job(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    priority: int = Field(ge=0, le=100)
    submitted_at: datetime
    status: JobStatus
    metadata: Dict[str, Any] = Field(default_factory=dict)
    last_status_change: datetime
    preemption_count: int = Field(default=0)
    wait_time_weight: float = Field(default=1.0)
    leader_id: Optional[str] = None

    @classmethod
    def create(cls, job_create: JobCreate) -> "Job":
        now = datetime.now(UTC)
        return cls(
            id=str(uuid.uuid4()),
            name=job_create.name,
            priority=job_create.priority,
            submitted_at=now,
            status=JobStatus.SUBMITTED,
            metadata=job_create.metadata,
            last_status_change=now,
        )

    def update_status(self, new_status: JobStatus) -> None:
        self.status = new_status
        self.last_status_change = datetime.now(UTC)

    def increment_preemption(self) -> None:
        self.preemption_count += 1
        self.update_status(JobStatus.PREEMPTED)

    def calculate_wait_time(self) -> float:
        """Calculate the total wait time in hours."""
        return (datetime.now(UTC) - self.submitted_at.astimezone(UTC)).total_seconds() / 3600
