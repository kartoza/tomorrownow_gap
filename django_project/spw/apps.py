# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: AppConfig for SPW
"""
from django.apps import AppConfig


class SpwConfig(AppConfig):
    """SPW App Config."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'spw'

    def ready(self):
        """App ready handler."""
        from spw.tasks import cleanup_r_execution_logs  # noqa
