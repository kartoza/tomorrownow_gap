# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Job Routes
"""

from fastapi import APIRouter, Depends, Query

from app.services.job_service import JobService
from app.models.job import JobStatusResponse
from app.core.exceptions import JobNotFoundError
from app.core.config import get_settings

router = APIRouter()


def get_job_service() -> JobService:
    """Dependency to get JobService instance."""
    return JobService()


@router.get("/{job_id}/wait")
async def wait_for_job_completion(
    job_id: str,
    max_wait_time: int = Query(default=300, ge=10, le=3600),
    poll_interval: float = Query(default=1, ge=0.5, le=60),
    job_service: JobService = Depends(get_job_service)
):
    """Wait for job completion via polling."""
    return await job_service.wait_for_completion(
        job_id,
        max_wait_time,
        poll_interval
    )


@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    job_service: JobService = Depends(get_job_service)
):
    """Get current job status."""
    job_data = await job_service.get_job_data(job_id)
    if not job_data:
        raise JobNotFoundError(job_id)

    settings = get_settings()
    return JobStatusResponse(
        job_id=job_id,
        worker_id=settings.WORKER_ID,
        **job_data.model_dump()
    )
