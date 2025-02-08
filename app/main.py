from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import routes

app = FastAPI(title="Job Priority Queue", version="1.0.0")

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
