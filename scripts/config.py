# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Scripts - Configuration
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuration settings for the scripts."""

    S3_ACCESS_KEY_ID: str
    S3_SECRET_ACCESS_KEY: str
    S3_ENDPOINT_URL: str
    S3_BUCKET_NAME: str
    S3_REGION_NAME: str

    # GAP Products
    SPW_GEOPARQUET_PATH: str
    DCAS_GEOPARQUET_PATH: str
    DAILY_FORECAST_ZARR_PATH: str

    # DUCKDB Configuration
    DUCKDB_NUM_THREADS: int = 2
    DUCKDB_MEMORY_LIMIT: str = '1GB'
    DUCKDB_USE_SSL: bool = True


_settings = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings(_env_file='.env')
    return _settings
