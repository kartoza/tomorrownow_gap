# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Core Tasks
"""

from celery import shared_task
import datetime
import logging
from django.utils import timezone
from django.core.mail import send_mail
from django.conf import settings

from core.models.background_task import BackgroundTask, TaskStatus
from core.utils.emails import get_admin_emails


logger = logging.getLogger(__name__)


@shared_task(name='check_running_tasks')
def check_running_tasks():
    """Check for running tasks every 6 hours."""
    # TODO: we could improve by storing the tasks to check in table
    tasks_to_check = [
        'generate_insight_report',
        'ingestor_session',
        'collector_session',
        'run_dcas',
        'run_daily_ingestor',
        'retry_crop_plan_generators',
        'tio_collector_session',
        'salient_collector_session'
    ]
    # find task that has started more than 6 hours ago and its still running
    tasks = BackgroundTask.objects.filter(
        task_name__in=tasks_to_check,
        status=TaskStatus.RUNNING,
        started_at__lt=timezone.now() - datetime.timedelta(hours=6),
        started_at__date=timezone.now().date()
    )
    running_tasks = []
    for task in tasks:
        total_running_time = timezone.now() - task.started_at
        # Format to HH:MM:SS
        formatted_running_time = str(total_running_time).split('.')[0]
        running_tasks.append(
            f'{task.task_name} - {task.task_id} - {formatted_running_time} - '
            f'{task.parameters}'
        )
        logger.info(
            f"Task {task.task_name} - {task.task_id} has been running for "
            f"{formatted_running_time}"
        )

    admin_emails = get_admin_emails()
    if running_tasks and admin_emails:
        # Send an email notification to admins
        send_mail(
            subject="Running Tasks Alert",
            message=(
                "The following tasks have been running "
                "for more than 6 hours:\n\n"
                + "\n".join(running_tasks)
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=admin_emails,
            fail_silently=False,
        )
        logger.info(f"Sent running tasks email to {admin_emails}")
