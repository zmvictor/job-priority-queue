import pytest
import asyncio
import os
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine
from app.models.database import Base, get_session, init_db, JobModel

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

@pytest.fixture
async def test_client():
    """Get a test client with initialized HA scheduler."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.core.queue_manager import QueueManager
    from app.core.ha_scheduler import HAGlobalScheduler
    
    # Initialize queue manager
    queue_manager = QueueManager()
    await queue_manager.start()
    app.state.queue_manager = queue_manager
    
    # Initialize HA scheduler
    ha_scheduler = HAGlobalScheduler("test-node", queue_manager)
    await ha_scheduler.start()
    app.state.ha_scheduler = ha_scheduler
    
    async with TestClient(app) as client:
        yield client
        
    await ha_scheduler.stop()
    await queue_manager.stop()
