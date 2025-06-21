# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Tasks for Executing Jobs
"""

import logging
import time
from core.celery import app
from django.utils import timezone

from core.models.background_task import TaskStatus
from gap.models import Preferences
from gap_api.models import Job, JobType


logger = logging.getLogger(__name__)


class BaseJobExecutor:
    """Base class for executing jobs."""

    DEFAULT_WAIT_TIME = 20 * 60  # 20 minutes
    DEFAULT_WAIT_SLEEP = 0.5  # 0.5 seconds

    def __init__(self, job: Job, is_main_executor=False):
        self.job = job
        self.job_config = Preferences.load().job_executor_config.get(
            job.job_type, {}
        )
        self.wait_time = self.job_config.get(
            'wait_time', self.DEFAULT_WAIT_TIME
        )
        self.wait_sleep = self.job_config.get(
            'wait_sleep', self.DEFAULT_WAIT_SLEEP
        )
        self.is_main_executor = is_main_executor

    def _pre_run(self):
        """Pre-run setup for the job."""
        pass

    def _post_run(self):
        """Post-run cleanup for the job."""
        pass

    def _submit_job(self):
        """Submit job to celery task."""
        raise NotImplementedError(
            "Subclasses must implement the _submit_job method."
        )

    def _run(self):
        """Run the job execution logic."""
        raise NotImplementedError("Subclasses must implement this method.")

    def _wait_for_completion(self):
        """Wait for the job to complete."""
        start_time = time.time()
        while time.time() - start_time < self.wait_time:
            self.job.refresh_from_db()
            if self.job.status in [TaskStatus.COMPLETED, TaskStatus.STOPPED]:
                return True
            if self.job.wait_type == 1:
                time.sleep(self.wait_sleep)
            # TODO: wait using asleep
        logger.warning(
            f"Job {self.job.id} did not complete within "
            f"the wait time {self.wait_time}."
        )
        return False

    def run(self):
        """Run the job execution."""
        self._pre_run()
        try:
            if self.is_main_executor:
                self._run()
            elif not self.job.is_async():
                self._run()
            else:
                self._submit_job()
                if self.job.wait_type > 0:
                    is_finished = self._wait_for_completion()
                    if not is_finished:
                        raise TimeoutError(
                            f"Job {self.job.id} did not complete in time."
                        )
        except Exception as e:
            logger.error(
                f"Error executing job {self.job.id}: {e}",
                exc_info=True
            )
            raise e
        finally:
            self._post_run()


class DataRequestJobExecutor(BaseJobExecutor):
    """Executor for Data Request jobs."""

    def _submit_job(self):
        """Submit job to celery task."""
        execute_data_request_job.apply_async(
            args=[self.job.id],
            queue=self.job.queue_name or 'default'
        )

    def _run(self):
        pass


@app.task(name='execute_data_request_job')
def execute_data_request_job(job_id):
    """Execute job for Data Request."""
    job = Job.objects.get(id=job_id)
    if job.job_type != JobType.DATA_REQUEST:
        raise ValueError("Job type is not Data Request")

    # update job status to running
    job.status = TaskStatus.RUNNING
    job.started_at = timezone.now()
    job.finished_at = None
    job.errors = None
    job.save()

    try:
        executor = DataRequestJobExecutor(job, is_main_executor=True)
        executor.run()
    except Exception as e:
        job.status = TaskStatus.STOPPED
        job.errors = str(e)
        job.finished_at = timezone.now()
        job.save()
        raise e
    else:
        job.status = TaskStatus.COMPLETED
        job.finished_at = timezone.now()
        job.save()
