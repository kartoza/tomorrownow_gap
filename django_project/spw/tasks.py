# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Tasks
"""
from datetime import timedelta
from celery import shared_task
from celery.utils.log import get_task_logger
from django.utils import timezone

from spw.models import RModelExecutionLog
from spw.utils.plumber import (
    kill_r_plumber_process,
    spawn_r_plumber,
    write_plumber_file
)


logger = get_task_logger(__name__)
REMOVE_AFTER_DAYS = 30


@shared_task(name="start_plumber_process")
def start_plumber_process():
    """Start plumber process when there is R code change."""
    logger.info('Starting plumber process')
    # kill existing process
    kill_r_plumber_process()
    # Generate plumber.R file
    write_plumber_file()
    # spawn the process
    plumber_process = spawn_r_plumber()
    if plumber_process:
        logger.info(f'plumber process pid {plumber_process.pid}')
    else:
        raise RuntimeError('Cannot execute plumber process!')


@shared_task(name="cleanup_r_execution_logs")
def cleanup_r_execution_logs():
    """Cleanup past RModelExecutionLog."""
    datetime_filter = timezone.now() - timedelta(days=REMOVE_AFTER_DAYS)
    RModelExecutionLog.objects.filter(
        start_date_time__lte=datetime_filter
    ).delete()
