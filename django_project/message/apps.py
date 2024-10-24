# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message Config

    A comprehensive framework of messages,
    categorized by group and application,
    designed to be utilized in a scheduled system for distribution to other
    users, sms, or appending to some API or files.
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class MessageConfig(AppConfig):
    """App Config for Message."""

    name = 'message'
    verbose_name = _('message')

    def ready(self):
        """App ready handler."""
        pass
