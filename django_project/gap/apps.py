# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: App Config
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class GAPConfig(AppConfig):
    """App Config for GroundObservations."""

    name = 'gap'
    verbose_name = _('Global Access Platform')

    def ready(self):
        """App ready handler."""
        from gap.tasks.crop_insight import generate_crop_plan  # noqa
        from salientsdk.login_api import download_query  # noqa
        from gap.utils.salient import patch_download_query
        import salientsdk.login_api
        salientsdk.login_api.download_query = patch_download_query

        # Import signals
        from gap.models.signup_request import notify_user_managers_on_signup # noqa
