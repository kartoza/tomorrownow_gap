# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: App Config for GAP API
"""

from django.apps import AppConfig


class GapApiConfig(AppConfig):
    """App Config for GAP API."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'gap_api'

    def ready(self):
        """App ready handler."""
        from gap_api.tasks import store_api_logs  # noqa
