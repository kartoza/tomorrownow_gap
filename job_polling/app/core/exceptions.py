# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Exception Handling
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging

logger = logging.getLogger(__name__)


class JobNotFoundError(Exception):
    """Exception raised when a job is not found."""

    def __init__(self, job_id: str):
        """Initialize the exception with the job ID."""
        self.job_id = job_id
        super().__init__(f"Job {job_id} not found")


class JobTimeoutError(Exception):
    """Exception raised when a job times out."""

    def __init__(self, job_id: str, timeout: int):
        """Initialize the exception with the job ID and timeout duration."""
        self.job_id = job_id
        self.timeout = timeout
        super().__init__(f"Job {job_id} timed out after {timeout} seconds")


class JobFailedError(Exception):
    """Exception raised when a job fails."""

    def __init__(self, job_id: str, status: str):
        """Initialize the exception with the job ID and failure status."""
        self.job_id = job_id
        self.status = status
        super().__init__(f"Job {job_id} failed with status: {status}")


def setup_exception_handlers(app: FastAPI):
    """Setup custom exception handlers"""

    @app.exception_handler(JobNotFoundError)
    async def job_not_found_handler(request: Request, exc: JobNotFoundError):
        """Handle JobNotFoundError exceptions."""
        return JSONResponse(
            status_code=404,
            content={"detail": str(exc), "job_id": exc.job_id}
        )

    @app.exception_handler(JobTimeoutError)
    async def job_timeout_handler(request: Request, exc: JobTimeoutError):
        """Handle JobTimeoutError exceptions."""
        return JSONResponse(
            status_code=504,
            content={
                "detail": str(exc),
                "job_id": exc.job_id,
                "timeout": exc.timeout
            }
        )

    @app.exception_handler(JobFailedError)
    async def job_failed_handler(request: Request, exc: JobFailedError):
        """Handle JobFailedError exceptions."""
        return JSONResponse(
            status_code=422,
            content={
                "detail": str(exc),
                "job_id": exc.job_id,
                "status": exc.status
            }
        )
