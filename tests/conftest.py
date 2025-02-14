import pytest
import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from app.models.database import Base, get_session, init_db

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(autouse=True)
async def setup_database():
    """Initialize test database before each test."""
    # Use in-memory SQLite for tests
    os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    
    # Initialize database
    await init_db()
    
    # Create leader record
    async with get_session() as session:
        leader = JobModel(
            id="leader",
            name="leader",
            status="pending",
            priority=0,
            job_metadata="{}",
            submitted_at=datetime.now(timezone.utc),
            last_status_change=datetime.now(timezone.utc),
            leader_id=None,
            last_heartbeat=None
        )
        session.add(leader)
        await session.commit()
    
    yield
    
    # Cleanup after test
    async with get_session() as session:
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        await session.commit()

@pytest.fixture
async def test_session():
    """Get a test database session."""
    async with get_session() as session:
        yield session
        await session.rollback()
