from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import routes
from app.core.queue_manager import queue_manager
from app.core.ha_scheduler import HAGlobalScheduler
from contextlib import asynccontextmanager
import uuid


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    from app.models.database import init_db
    await init_db()
    
    # Create HA scheduler with unique node ID
    node_id = str(uuid.uuid4())
    ha_scheduler = HAGlobalScheduler(node_id, queue_manager)
    await ha_scheduler.start()
    
    # Store scheduler in app state
    app.state.ha_scheduler = ha_scheduler

    # Yield control back to FastAPI (app will run after this)
    yield

    # Shutdown code (runs after the application stops)
    await ha_scheduler.stop()
    await queue_manager.clear()
    await queue_manager.stop()

app = FastAPI(title="Job Priority Queue", version="1.0.0", lifespan=lifespan)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include job queue routes
app.include_router(routes.router, prefix="/api/v1", tags=["jobs"])

@app.get("/healthz")
async def healthz():
    """Health check endpoint with leader status."""
    return {
        "status": "ok",
        "is_leader": app.state.ha_scheduler.is_leader,
        "node_id": app.state.ha_scheduler.node_id
    }
