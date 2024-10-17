# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for API Tracking
"""

from django.db import models
from django.conf import settings
from rest_framework_tracking.base_models import BaseAPIRequestLog


class APIRequestLog(BaseAPIRequestLog):
    """Models that stores GAP API request log."""

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='user_api'
    )

    query_params = models.JSONField(
        default=dict,
        null=True,
        blank=True
    )
