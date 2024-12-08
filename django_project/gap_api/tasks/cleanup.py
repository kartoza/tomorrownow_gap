# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Tasks for Cleanup Expired Data
"""

from celery import shared_task
from datetime import timedelta
from django.utils import timezone

from gap_api.models import Location, UserFile


@shared_task(name="cleanup_user_locations")
def cleanup_user_locations():
    """Cleanup Expired Location."""
    Location.objects.filter(
        expired_on__lte=timezone.now()
    ).delete()


@shared_task(name="cleanup_user_files")
def cleanup_user_files():
    """Cleanup UserFile more than 2 days."""
    UserFile.objects.filter(
        created_on__lte=(timezone.now() - timedelta(days=2))
    ).delete()
