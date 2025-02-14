import pytest
import asyncio
import os
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.models.database import Base, get_session, init_db, JobModel, JobStatusEnum
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.queue_manager import QueueManager
from app.core.ha_scheduler import HAGlobalScheduler

@pytest.fixture(scope="function")
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
        try:
            now = datetime.now(timezone.utc)
            leader = JobModel(
                id="leader",
                name="leader",
                status=JobStatusEnum.SUBMITTED,  # Use enum value
                priority=0,
                job_metadata="{}",
                submitted_at=now,
                last_status_change=now,
                leader_id=None,
                last_heartbeat=None,
                preemption_count=0,
                wait_time_weight=1.0
            )
            session.add(leader)
            await session.commit()
        except Exception as e:
            print(f"Error creating leader record: {str(e)}")
            await session.rollback()
    
    yield
    
    # Cleanup after test
    async with get_session() as session:
        try:
            for table in reversed(Base.metadata.sorted_tables):
                await session.execute(text("DELETE FROM " + table.name))
            await session.commit()
        except Exception as e:
            print(f"Error cleaning up database: {str(e)}")
            await session.rollback()

@pytest.fixture
async def test_session():
    """Get a test database session."""
    async with get_session() as session:
        yield session
        await session.rollback()

@pytest.fixture
async def test_client():
    """Get a test client with initialized HA scheduler."""
    # Initialize queue manager
    queue_manager = QueueManager()
    await queue_manager.start()
    
    # Initialize HA scheduler with unique node ID
    ha_scheduler = HAGlobalScheduler("test-node", queue_manager)
    await ha_scheduler.start()
    
    # Force leader status for tests
    ha_scheduler.is_leader = True
    
    # Store in app state
    app.state.queue_manager = queue_manager
    app.state.ha_scheduler = ha_scheduler
    
    # Create test client
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    yield client
    
    # Cleanup
    await client.aclose()
    await ha_scheduler.stop()
    await queue_manager.stop()
