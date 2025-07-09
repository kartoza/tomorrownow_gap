# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Configuration
"""

from pydantic_settings import BaseSettings
from typing import List
import os


class Settings(BaseSettings):
    """Configuration settings for the Job Polling API."""

    # Project info
    PROJECT_NAME: str = "Job Polling API"
    VERSION: str = "1.0.0"
    DEBUG: bool = False
    DEBUG_FULL_RESPONSE: bool = False

    # Redis configuration
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""

    # API configuration
    ALLOWED_HOSTS: List[str] = ["*"]
    WORKER_ID: str = f"worker-{os.getpid()}"

    # Logging
    LOG_LEVEL: str = "INFO"
    SENTRY_DSN: str = ""


_settings = None


def get_settings() -> Settings:
    """Get the singleton instance of Settings."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
