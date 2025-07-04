# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Job Data Models
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from enum import Enum


class JobStatus(str, Enum):
    """Enumeration for job statuses."""

    PENDING = "Pending"
    RUNNING = "Running"
    COMPLETED = "Completed"
    STOPPED = "Stopped"


class JobData(BaseModel):
    """Data model for job information."""

    status: JobStatus
    errors: Optional[str] = None
    url: Optional[str] = ""
    output_json: Optional[dict] = None
    content_type: Optional[str] = None
    file_name: Optional[str] = None
    updated_on: Optional[int] = None


class JobWaitRequest(BaseModel):
    """Request model for waiting on a job."""

    max_wait_time: int = Field(default=300, ge=10, le=3600)
    poll_interval: float = Field(default=1, ge=0.5, le=60)


class JobWaitResponse(BaseModel):
    """Response model for job wait operation."""

    job_id: str
    status: JobStatus
    errors: Optional[str] = None
    url: Optional[str] = ""
    output_json: Optional[dict] = None
    content_type: Optional[str] = None
    file_name: Optional[str] = None
    updated_on: Optional[int] = None
    worker_id: str
    polling_time: float
    polls_count: int


class JobStatusResponse(BaseModel):
    """Response model for job status."""

    job_id: str
    worker_id: str
    status: JobStatus
    url: Optional[str]
    output_json: Optional[dict] = None
    content_type: Optional[str] = None
    file_name: Optional[str] = None
    updated_on: Optional[int] = None


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    worker_id: str
    redis_connected: bool
    timestamp: datetime
