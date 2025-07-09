# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Redis Connection
"""

import redis.asyncio as redis
from app.core.config import get_settings

_redis_client = None


async def get_redis() -> redis.Redis:
    """Get Redis client instance."""
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD or None,
            decode_responses=True,
            retry_on_timeout=True,
        )
    return _redis_client


async def init_redis():
    """Initialize Redis connection."""
    redis_client = await get_redis()
    await redis_client.ping()


async def close_redis():
    """Close Redis connection."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
