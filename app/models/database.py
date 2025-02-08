from datetime import datetime, UTC
from sqlalchemy import Column, Integer, String, Float, DateTime, Enum, TypeDecorator, event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import NullPool
import enum

class UTCDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            return value
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return value.replace(tzinfo=UTC)
        return value

Base = declarative_base()

class JobStatusEnum(str, enum.Enum):
    SUBMITTED = "submitted"
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PREEMPTED = "preempted"

class JobModel(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    priority = Column(Integer, nullable=False)
    submitted_at = Column(UTCDateTime, nullable=False)
    status = Column(Enum(JobStatusEnum), nullable=False)
    job_metadata = Column(String, nullable=False)  # JSON string
    last_status_change = Column(UTCDateTime, nullable=False)
    preemption_count = Column(Integer, default=0)
    wait_time_weight = Column(Float, default=1.0)
    leader_id = Column(String, nullable=True)  # For HA - which node owns this job

# Async database setup
engine = create_async_engine(
    "sqlite+aiosqlite:///:memory:",  # Use in-memory database for tests
    echo=True,
    connect_args={
        "check_same_thread": False,
    }
)

async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

def get_session() -> AsyncSession:
    return async_session()
