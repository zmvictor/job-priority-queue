from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, Mapping
from pydantic import BaseModel, Field, ConfigDict, model_validator, model_serializer
import uuid
import json
from types import MappingProxyType

class JobJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, MappingProxyType):
            return dict(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)

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
    model_config = ConfigDict(
        from_attributes=True,
        validate_assignment=True,
        frozen_default=False,
        extra='forbid'
    )

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'priority': self.priority,
            'submitted_at': self.submitted_at.isoformat(),
            'status': self.status.value,
            'metadata': dict(self.metadata),
            'last_status_change': self.last_status_change.isoformat(),
            'preemption_count': self.preemption_count,
            'wait_time_weight': self.wait_time_weight,
            'leader_id': self.leader_id
        }

    id: str
    name: str
    priority: int = Field(ge=0, le=100)
    submitted_at: datetime
    status: JobStatus
    metadata: Dict[str, Any] = Field(default_factory=dict)
    last_status_change: datetime
    preemption_count: int = Field(default=0)
    wait_time_weight: float = Field(default=1.0)
    leader_id: Optional[str] = Field(default=None)
    credit: float = Field(default=0.0)

    @model_validator(mode='after')
    def make_metadata_immutable(self) -> 'Job':
        """Make metadata immutable after model creation."""
        object.__setattr__(self, 'metadata', MappingProxyType(self.metadata))
        return self

    def get_metadata(self) -> Dict[str, Any]:
        """Get a copy of the metadata to ensure immutability."""
        return dict(self.metadata)

    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Override model_dump to handle MappingProxyType serialization."""
        data = super().model_dump(**kwargs)
        if isinstance(data.get('metadata'), MappingProxyType):
            data['metadata'] = dict(data['metadata'])
        return data

    def model_dump_json(self, **kwargs) -> str:
        """Override model_dump_json to handle MappingProxyType serialization."""
        data = self.model_dump(**kwargs)
        return json.dumps(data, cls=JobJSONEncoder)

    def calculate_wait_time(self) -> float:
        """Calculate the total wait time in hours."""
        # Ensure both datetimes are timezone-aware and in UTC
        now = datetime.now(timezone.utc)
        submitted = self.submitted_at.astimezone(timezone.utc)
        wait_time = (now - submitted).total_seconds() / 3600.0
        # For test stability, use the actual submitted_at time
        if wait_time < 0:  # If submitted_at is in the future
            return 0.0  # No negative wait times
        return wait_time

    def update_wait_time_weight(self) -> None:
        """Update wait time weight based on time since submission."""
        wait_time = self.calculate_wait_time()
        # Linear boost: 1.0 base + wait_time/24 boost
        # For 24h old job, weight should be 2.0
        # For 12h old job, weight should be 1.5
        boost = 1.0 + (wait_time / 24.0)  # Base + age in days
        object.__setattr__(self, 'wait_time_weight', boost)
        # Update submitted_at to ensure it's timezone-aware
        object.__setattr__(self, 'submitted_at', self.submitted_at.astimezone(timezone.utc))
        # Set initial credit to 0
        if not hasattr(self, 'credit'):
            object.__setattr__(self, 'credit', 0.0)

    @classmethod
    def create(cls, job_create: JobCreate) -> "Job":
        now = datetime.now(timezone.utc)
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
        """Update job status and last_status_change."""
        object.__setattr__(self, 'status', new_status)
        object.__setattr__(self, 'last_status_change', datetime.now(timezone.utc))

    def increment_preemption(self) -> None:
        """Increment preemption count and update status."""
        object.__setattr__(self, 'preemption_count', self.preemption_count + 1)
        self.update_status(JobStatus.PREEMPTED)
