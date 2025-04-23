# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: App Config
"""

from django.apps import AppConfig

class CoreConfig(AppConfig):
    """App Config for Core."""

    name = 'core'

    def ready(self):
        """App ready handler."""
        from core.tasks import check_running_tasks  # noqa
