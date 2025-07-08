# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Job Polling API - Job Service
"""
import json
import asyncio
import time
from typing import Optional
from urllib.parse import urlparse
from fastapi.responses import Response

from app.core.database import get_redis
from app.core.config import get_settings
from app.core.exceptions import (
    JobNotFoundError,
    JobTimeoutError,
    JobFailedError
)
from app.models.job import JobData, JobStatus, JobWaitResponse


class JobService:
    """Service for managing job data and polling."""

    def __init__(self):
        """Initialize JobService with settings."""
        self.settings = get_settings()

    async def get_job_data(self, job_id: str) -> Optional[JobData]:
        """Get job data from Redis."""
        redis_client = await get_redis()
        key = f"job:{job_id}"

        try:
            value = await redis_client.get(key)
            if value:
                data = json.loads(value)
                return JobData(**data)
            return None
        except Exception:
            return None

    async def delete_job(self, job_id: str) -> bool:
        """Delete job from Redis."""
        redis_client = await get_redis()
        key = f"job:{job_id}"

        try:
            result = await redis_client.delete(key)
            return result > 0
        except Exception:
            return False

    def prepare_response(
        self, job_id, data: JobData,
        polling_time: float, polls_count: int
    ):
        """Prepare response object."""
        if self.settings.DEBUG_FULL_RESPONSE:
            # return full response if debug mode is enabled
            return JobWaitResponse(
                job_id=job_id,
                status=data.status,
                errors=data.errors,
                url=data.url,
                output_json=data.output_json,
                content_type=data.content_type,
                file_name=data.file_name,
                updated_on=data.updated_on,
                worker_id=self.settings.WORKER_ID,
                polling_time=polling_time,
                polls_count=polls_count
            )
        elif data.content_type == "application/json":
            # return output_Json
            return data.output_json
        else:
            # return Response with x accel redirect
            parse_result = urlparse(data.url)
            url = data.url.replace(
                f"{parse_result.scheme}://{parse_result.netloc}/",
                ""
            )
            headers = {
                "X-Accel-Redirect": (
                    f'/userfiles/{parse_result.scheme}/'
                    f'{parse_result.netloc}/{url}'
                ),
                "Content-Type": data.content_type,
                "Content-Disposition": (
                    f"attachment; filename={data.file_name}"
                )
            }
            return Response(content="", headers=headers)

    async def wait_for_completion(
        self,
        job_id: str,
        max_wait_time: int,
        poll_interval: float
    ) -> JobWaitResponse:
        """Poll job until completion or timeout."""
        start_time = time.time()
        poll_count = 0

        # Initial check
        initial_data = await self.get_job_data(job_id)
        if initial_data is None:
            # Wait briefly in case job was just created
            await asyncio.sleep(1)
            initial_data = await self.get_job_data(job_id)

        if initial_data is None:
            raise JobNotFoundError(job_id)

        # Check if already completed
        if initial_data.status == JobStatus.COMPLETED:
            if not initial_data.url and not initial_data.output_json:
                raise JobFailedError(job_id, "COMPLETED_NO_DATA")

            return self.prepare_response(
                job_id,
                initial_data,
                polling_time=0.0,
                polls_count=1
            )

        # Start polling
        while True:
            poll_count += 1
            elapsed_time = time.time() - start_time

            # Check timeout
            if elapsed_time >= max_wait_time:
                raise JobTimeoutError(job_id, max_wait_time)

            # Get current job data
            job_data = await self.get_job_data(job_id)

            if job_data:
                if job_data.status == JobStatus.COMPLETED:
                    if not job_data.url and not job_data.output_json:
                        raise JobFailedError(job_id, "COMPLETED_NO_DATA")

                    return self.prepare_response(
                        job_id,
                        job_data,
                        polling_time=round(elapsed_time, 1),
                        polls_count=poll_count
                    )
                elif job_data.status in [JobStatus.STOPPED]:
                    raise JobFailedError(job_id, job_data.status.value)

            # Wait before next poll
            await asyncio.sleep(poll_interval)
