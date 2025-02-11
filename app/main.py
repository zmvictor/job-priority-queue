from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import routes
from app.core.queue_manager import queue_manager
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    from app.models.database import init_db
    await init_db()
    await queue_manager.start()

    # Yield control back to FastAPI (app will run after this)
    yield

    # Shutdown code (runs after the application stops)
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
    return {"status": "ok"}
