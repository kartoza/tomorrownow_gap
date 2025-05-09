# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS AppConfig
"""
from django.apps import AppConfig


class DcasConfig(AppConfig):
    """App Config for DCAS."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'dcas'

    def ready(self):
        """App ready handler."""
        from dcas.tasks import run_dcas, export_dcas_sftp, export_dcas_minio, cleanup_dcas_old_output_files  # noqa
