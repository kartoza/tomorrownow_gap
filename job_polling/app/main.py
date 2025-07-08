# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Application Entry Point
"""
from fastapi import FastAPI
import logging
import sentry_sdk
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from app.core.config import get_settings
from app.core.database import init_redis
from app.core.exceptions import setup_exception_handlers
from app.api.routes import jobs, health, admin

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.PROJECT_NAME,
        description="A reliable job status polling API",
        version=settings.VERSION,
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
    )

    # Exception handlers
    setup_exception_handlers(app)

    # Sentry integration
    if settings.SENTRY_DSN:
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            traces_sample_rate=1.0,
            send_default_pii=True,
        )
        app.add_middleware(SentryAsgiMiddleware)

    # Include routers
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(jobs.router, prefix="/job", tags=["jobs"])
    app.include_router(admin.router, prefix="/admin", tags=["admin"])

    # Startup and shutdown events
    @app.on_event("startup")
    async def startup_event():
        logger.info(f"Starting {settings.PROJECT_NAME}")
        await init_redis()

    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("Shutting down application")

    return app


app = create_app()
