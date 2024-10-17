"""Tomorrow Now GAP."""
from __future__ import absolute_import, unicode_literals

import logging
import os

from celery import Celery, signals
from celery.result import AsyncResult
from celery.schedules import crontab
from celery.utils.serialization import strtobool
from celery.worker.control import inspect_command
from django.utils import timezone

logger = logging.getLogger(__name__)

# set the default Django settings module for the 'celery' program.
# this is also used in manage.py
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

# Get the base REDIS URL, default to redis' default
BASE_REDIS_URL = (
    f'redis://default:{os.environ.get("REDIS_PASSWORD", "")}'
    f'@{os.environ.get("REDIS_HOST", "")}',
)

app = Celery('GAP')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# use max task = 1 to avoid memory leak from numpy/ingestor
app.conf.worker_max_tasks_per_child = 1

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

app.conf.broker_url = BASE_REDIS_URL

# this allows you to schedule items in the Django admin.
app.conf.beat_scheduler = 'django_celery_beat.schedulers.DatabaseScheduler'

# Task cron job schedules
app.conf.beat_schedule = {
    'generate-crop-plan': {
        'task': 'generate_crop_plan',
        # Run everyday at 01:30 UTC or 04:30 EAT
        'schedule': crontab(minute='30', hour='1'),
    },
    'retry-crop-plan-generators': {
        'task': 'retry_crop_plan_generators',
        'schedule': crontab(minute='0', hour='*'),
    },
    'salient-collector-session': {
        'task': 'salient_collector_session',
        # Run every Monday 02:00 UTC
        'schedule': crontab(minute='0', hour='4', day_of_week='1'),
    },
    'tio-collector-session': {
        'task': 'tio_collector_session',
        # Run everyday at 00:30 UTC
        'schedule': crontab(minute='30', hour='00'),
    },
    # Run all ingestor session daily
    'run-daily-ingestor-session': {
        'task': 'run_daily_ingestor',
        # Run everyday at 00:00 UTC
        'schedule': crontab(minute='00', hour='00'),
    },
    'cleanup-r-execution-logs': {
        'task': 'cleanup_r_execution_logs',
        # Run every first day of each month at 00:00 UTC
        'schedule': crontab(minute=0, hour=0, day_of_month=1),
    }
}


def cancel_task(task_id: str):
    """
    Cancel task if it's ongoing.

    :param task_id: task identifier
    """
    try:
        res = AsyncResult(task_id)
        if not res.ready():
            # find if there is running task and stop it
            app.control.revoke(
                task_id,
                terminate=True,
                signal='SIGKILL'
            )
    except Exception as ex:
        logger.error(f'Failed cancel_task: {task_id}')
        logger.error(ex)


def check_ongoing_task(task_name: str, context_id: str) -> bool:
    """Check whether a task_name is still running for the same context.

    This method can be useful to prevent system running the same task while
    the worker is still processing the first one.
    :param task_name: name of the task
    :type task_name: str
    :param context_id: main id for the task
    :type context_id: str
    :return: True if there is still running task with the same context
    :rtype: bool
    """
    from core.models import BackgroundTask, TaskStatus
    bg_task = BackgroundTask.objects.filter(
        task_name=task_name,
        context_id=context_id
    ).first()
    if bg_task is None:
        return False
    return bg_task.status in [
        TaskStatus.RUNNING, TaskStatus.QUEUED
    ]


def update_task_progress(
        task_name: str, context_id: str, progress: float,
        progress_text: str):
    """Update the BackgroundTask record with progress from running task.

    :param task_name: name of the task
    :type task_name: str
    :param context_id: main id for the task
    :type context_id: str
    :param progress: progress value
    :type progress: float
    :param progress_text: progress text
    :type progress_text: str
    """
    from core.models import BackgroundTask
    bg_task = BackgroundTask.objects.filter(
        task_name=task_name,
        context_id=context_id
    ).first()
    if bg_task is None:
        return
    bg_task.progress = progress
    bg_task.progress_text = progress_text
    bg_task.last_update = timezone.now()
    bg_task.save(update_fields=[
        'progress', 'progress_text', 'last_update'
    ])


EXCLUDED_TASK_LIST = [
    'celery.backend_cleanup'
]


def is_task_ignored(task_name: str) -> bool:
    """Check if task should be ignored.

    :param task_name: name of the task
    :type task_name: str
    :return: True if task is not empty and not in excluded list
    :rtype: bool
    """
    return task_name == '' or task_name in EXCLUDED_TASK_LIST


@signals.after_task_publish.connect
def task_sent_handler(sender=None, headers=None, body=None, **kwargs):
    """Handle a task being sent to the celery.

    Task is sent to celery, but might not be queued to worker yet.
    :param sender: task sender, defaults to None
    :type sender: any, optional
    :param headers: task headers, defaults to None
    :type headers: dict, optional
    :param body: task body, defaults to None
    :type body: dict, optional
    """
    from core.models import BackgroundTask
    info = headers if 'task' in headers else body
    task_id = info['id']
    task_name = info['task']
    task_args = body[0]
    if is_task_ignored(task_name):
        return
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'last_update': timezone.now(),
            'parameters': task_args
        }
    )
    bg_task.task_on_sent(
        task_id, task_name, str(task_args)
    )


@signals.task_received.connect
def task_received_handler(sender, request=None, **kwargs):
    """Handle a task being queued in the worker.

    :param sender: task sender
    :type sender: any
    :param request: task request, defaults to None
    :type request: dict, optional
    """
    from core.models import BackgroundTask
    task_id = request.id if request else None
    task_args = request.args
    task_name = request.name if request else ''
    if is_task_ignored(task_name):
        return
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'last_update': timezone.now(),
            'parameters': str(task_args)
        }
    )
    bg_task.task_on_queued(
        task_id, task_name, str(task_args)
    )


@signals.task_prerun.connect
def task_prerun_handler(
        sender=None, task_id=None, task=None,
        args=None, **kwargs):
    """Handle when task is about to be executed.

    :param sender: task sender, defaults to None
    :type sender: any, optional
    :param task_id: task id, defaults to None
    :type task_id: str, optional
    :param task: task, defaults to None
    :type task: any, optional
    :param args: task args, defaults to None
    :type args: any, optional
    """
    from core.models import BackgroundTask
    task_name = sender.name if sender else ''
    if is_task_ignored(task_name):
        return
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'parameters': str(args),
            'last_update': timezone.now(),
        }
    )
    bg_task.task_on_started()


@signals.task_success.connect
def task_success_handler(sender, **kwargs):
    """Handle when task is successfully run.

    :param sender: task sender
    :type sender: any
    """
    from core.models import BackgroundTask
    task_name = sender.name if sender else ''
    if is_task_ignored(task_name):
        return
    task_id = sender.request.id
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'last_update': timezone.now(),
        }
    )
    bg_task.task_on_completed()


@signals.task_failure.connect
def task_failure_handler(
        sender, task_id=None, args=None,
        exception=None, traceback=None, **kwargs):
    """Handle a failure task from unexpected exception.

    :param sender: task sender
    :type sender: any
    :param task_id: task id, defaults to None
    :type task_id: str, optional
    :param args: task args, defaults to None
    :type args: any, optional
    :param exception: exception, defaults to None
    :type exception: Exception, optional
    :param traceback: traceback, defaults to None
    :type traceback: Traceback, optional
    """
    from core.models import BackgroundTask
    task_name = sender.name if sender else ''
    if is_task_ignored(task_name):
        return
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'parameters': str(args),
            'last_update': timezone.now(),
        }
    )
    bg_task.task_on_errors(exception, traceback)


@signals.task_revoked.connect
def task_revoked_handler(sender, request=None, **kwargs):
    """Handle a cancelled task.

    :param sender: task sender
    :type sender: any
    :param request: task request, defaults to None
    :type request: any, optional
    """
    from core.models import BackgroundTask
    task_name = sender.name if sender else ''
    task_id = request.id if request else None
    if is_task_ignored(task_name):
        return
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'last_update': timezone.now(),
        }
    )
    bg_task.task_on_cancelled()
    if bg_task.task_name == 'ingestor_session':
        from gap.ingestor.base import ingestor_revoked_handler
        ingestor_revoked_handler(bg_task)


@signals.task_internal_error.connect
def task_internal_error_handler(
        sender, task_id=None, exception=None, **kwargs):
    """Handle when task is error from internal celery error.

    :param sender: task sender
    :type sender: any
    :param task_id: task id, defaults to None
    :type task_id: str, optional
    :param exception: Exception, defaults to None
    :type exception: Exception, optional
    """
    from core.models import BackgroundTask
    task_name = sender.name if sender else ''
    if is_task_ignored(task_name):
        return
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'last_update': timezone.now(),
        }
    )
    bg_task.task_on_errors(exception)


@signals.task_retry.connect
def task_retry_handler(sender, reason, **kwargs):
    """Handle when task is being retried by celery.

    :param sender: task sender
    :type sender: any
    :param reason: retry reason
    :type reason: str
    """
    from core.models import BackgroundTask
    task_name = sender.name if sender else ''
    if is_task_ignored(task_name):
        return
    task_id = sender.request.id
    bg_task, _ = BackgroundTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            'task_name': task_name,
            'last_update': timezone.now(),
        }
    )
    bg_task.task_on_retried(reason)


@inspect_command(
    alias='dump_conf',
    signature='[include_defaults=False]',
    args=[('with_defaults', strtobool)],
)
def conf(state, with_defaults=False, **kwargs):
    """Disable the `conf` inspect command.

    This is to stop sensitive configuration info appearing in e.g. Flower.
    (Celery makes an attempt to remove sensitive info,but it is not foolproof)
    :param state: state
    :type state: any
    :param with_defaults: defaults, defaults to False
    :type with_defaults: bool, optional
    :return: Detail dictionary
    :rtype: dict
    """
    return {'error': 'Config inspection has been disabled.'}
