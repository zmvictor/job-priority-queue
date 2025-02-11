from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import routes
from app.core.queue_manager import queue_manager

app = FastAPI(title="Job Priority Queue", version="1.0.0")

@app.on_event("startup")
async def startup_event():
    from app.models.database import init_db
    await init_db()
    await queue_manager.start()


@app.on_event("shutdown")
async def shutdown_event():
    await queue_manager.clear()
    await queue_manager.stop()

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
