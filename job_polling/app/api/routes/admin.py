# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Admin Routes
"""

from fastapi import APIRouter
from typing import Dict, Any

from app.core.database import get_redis
from app.core.config import get_settings

router = APIRouter()


@router.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get job statistics."""
    settings = get_settings()
    redis_client = await get_redis()

    try:
        # Count jobs by status
        keys = await redis_client.keys("job:*")
        stats = {
            "total_jobs": len(keys),
            "by_status": {},
            "worker_id": settings.WORKER_ID
        }

        for key in keys:
            try:
                import json
                data = await redis_client.get(key)
                job_data = json.loads(data)
                status = job_data.get('status', 'UNKNOWN')
                stats["by_status"][status] = (
                    stats["by_status"].get(status, 0) + 1
                )
            except Exception:
                continue

        return stats
    except Exception as e:
        return {"error": str(e), "worker_id": settings.WORKER_ID}


@router.get("/info")
async def get_info() -> Dict[str, Any]:
    """Get application info."""
    settings = get_settings()
    return {
        "project_name": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "worker_id": settings.WORKER_ID,
        "debug": settings.DEBUG
    }
