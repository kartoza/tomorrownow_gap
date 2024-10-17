# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Tasks for API Tracking
"""

import json
from core.celery import app
from django.core.cache import cache
from django.contrib.auth import get_user_model
from django.utils.dateparse import parse_datetime

from gap.models import Preferences
from gap_api.models import APIRequestLog
from gap_api.mixins import GAPAPILoggingMixin


UserModel = get_user_model()


@app.task(name='store_api_logs', ignore_result=True)
def store_api_logs():
    """Store API Logs from Redis into database."""
    # pull logs from Redis
    batch_size = Preferences.load().api_log_batch_size
    logs = []
    for _ in range(batch_size):
        log_entry = cache._cache.get_client().lpop(
            GAPAPILoggingMixin.CACHE_KEY
        )
        if log_entry:
            entry = json.loads(log_entry)
            # parse requested_at
            if entry.get('requested_at', None):
                entry['requested_at'] = parse_datetime(entry['requested_at'])

            # parse user
            if entry.get('user', None):
                user_id = entry['user']
                entry['user'] = UserModel._default_manager.filter(
                    id=user_id).first()

            logs.append(APIRequestLog(**entry))

    # write logs to the database
    if logs:
        APIRequestLog.objects.bulk_create(logs)
