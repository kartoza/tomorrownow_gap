# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Cleanup Tasks.
"""

from celery.utils.log import get_task_logger
from core.celery import app
from django.utils import timezone
from datetime import timedelta

from gap.models.signup_request import SignUpRequest, RequestStatus

logger = get_task_logger(__name__)


@app.task(name='cleanup_incomplete_signups')
def cleanup_incomplete_signups():
    """Delete any SignUpRequest still INCOMPLETE older than 30 days."""
    cutoff = timezone.now() - timedelta(days=30)
    qs = SignUpRequest.objects.filter(
        status=RequestStatus.INCOMPLETE,
        submitted_at__lt=cutoff
    )
    count = qs.count()
    qs.delete()
    return f"Deleted {count} old incomplete SignUpRequests"



