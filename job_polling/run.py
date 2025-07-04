# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Application Runner
"""
import uvicorn


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,  # Only for development
        log_level="info"
    )
