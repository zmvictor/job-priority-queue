from fastapi import APIRouter, HTTPException, Request
from typing import List
import json
from sqlalchemy import select
from app.models.job import JobCreate, Job, JobStatus
from app.models.database import get_session, JobModel, JobStatusEnum
from app.core.queue_manager import queue_manager

# Re-export for type hints
__all__ = ['router']

router = APIRouter()

@router.post("/jobs", response_model=Job)
async def submit_job(job: JobCreate, request: Request):
    """Submit a new job to the priority queue."""
    ha_scheduler = request.app.state.ha_scheduler
    if not ha_scheduler.is_leader:
        raise HTTPException(status_code=503, detail="Not the leader node")
    return await ha_scheduler.submit_job(job)

@router.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str, request: Request):
    """Get job status by ID."""
    ha_scheduler = request.app.state.ha_scheduler
    job = await ha_scheduler.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@router.post("/jobs/{job_id}/preempt", response_model=Job)
async def preempt_job(job_id: str, request: Request):
    """Preempt a running job."""
    import logging
    logger = logging.getLogger(__name__)
    try:
        ha_scheduler = request.app.state.ha_scheduler
        if not ha_scheduler.is_leader:
            raise HTTPException(status_code=503, detail="Not the leader node")
            
        # Get job state first
        job = await ha_scheduler.get_job(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        if job.status != JobStatus.RUNNING:
            logger.error(f"Job {job_id} is not in RUNNING state (current status: {job.status})")
            raise HTTPException(
                status_code=400, 
                detail=f"Job {job_id} is not in RUNNING state (current status: {job.status})"
            )
        
        # Preempt job
        logger.info(f"Attempting to preempt job {job_id}")
        preempted_job = await queue_manager.preempt_job(job_id)
        logger.info(f"Successfully preempted job {job_id}")
        return preempted_job
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Failed to preempt job {job_id}: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        logger.error(f"Unexpected error while preempting job {job_id}: {error_detail}")
        logger.error(f"Job state: {job.__dict__ if job else 'None'}")
        raise HTTPException(status_code=500, detail=error_detail)

@router.get("/jobs", response_model=List[Job])
async def list_running_jobs(request: Request):
    """List all currently running jobs."""
    ha_scheduler = request.app.state.ha_scheduler
    return await ha_scheduler.queue_manager.get_running_jobs()

@router.post("/jobs/schedule", response_model=Job)
async def schedule_next_job(request: Request):
    """Schedule the next job from the queue."""
    ha_scheduler = request.app.state.ha_scheduler
    if not ha_scheduler.is_leader:
        raise HTTPException(status_code=503, detail="Not the leader node")
    job = await ha_scheduler.queue_manager.schedule_next_job()
    if not job:
        raise HTTPException(status_code=404, detail="No jobs in queue")
    return job

@router.post("/jobs/update-priorities")
async def update_job_priorities(request: Request):
    """Update priorities of all queued jobs based on wait time."""
    ha_scheduler = request.app.state.ha_scheduler
    if not ha_scheduler.is_leader:
        raise HTTPException(status_code=503, detail="Not the leader node")
    await ha_scheduler.queue_manager.update_priorities()
    return {"status": "ok"}
