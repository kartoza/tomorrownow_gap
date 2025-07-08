# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Health Check Routes
"""

from fastapi import APIRouter
from datetime import datetime

from app.core.database import get_redis
from app.core.config import get_settings
from app.models.job import HealthResponse

router = APIRouter()


@router.get("/", response_model=HealthResponse)
async def health_check():
    """Detailed health check."""
    settings = get_settings()

    try:
        redis_client = await get_redis()
        await redis_client.ping()
        redis_healthy = True
    except Exception:
        redis_healthy = False

    return HealthResponse(
        status="healthy" if redis_healthy else "unhealthy",
        worker_id=settings.WORKER_ID,
        redis_connected=redis_healthy,
        timestamp=datetime.now()
    )


@router.get("/ping")
async def ping():
    """Endpoint that returns pong."""
    return {"message": "pong"}
