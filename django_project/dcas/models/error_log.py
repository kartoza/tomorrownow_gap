# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Output
"""

from django.utils import timezone
from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _
from django.dispatch import receiver
from django.db.models.signals import post_delete

from core.models.background_task import TaskStatus
from gap.models.farm_registry import FarmRegistry
from dcas.models.request import DCASRequest


class DCASErrorType(models.TextChoices):
    """Enum for error types in DCAS ErrorLog."""

    MISSING_MESSAGES = "MISSING_MESSAGES", _("Missing Messages")
    PROCESSING_FAILURE = "PROCESSING_FAILURE", _("Processing Failure")
    FOUND_REPETITIVE = "FOUND_REPETITIVE", _("Found Repetitive")
    OTHER = "OTHER", _("Other")


class DCASErrorLog(models.Model):
    """Model to store farms that cannot be processed."""

    request = models.ForeignKey(
        DCASRequest, on_delete=models.CASCADE,
        related_name='error_logs',
        help_text="The DCAS request associated with this error."
    )
    farm_registry = models.ForeignKey(
        FarmRegistry, on_delete=models.CASCADE,
        help_text="The unique identifier of the farm that failed to process.",
        null=True,
        blank=True
    )
    error_type = models.CharField(
        max_length=50,
        choices=DCASErrorType.choices,
        default=DCASErrorType.OTHER,
        help_text="The type of error encountered."
    )
    error_message = models.TextField(
        help_text="Details about why the farm could not be processed.",
        null=True,
        blank=True
    )
    logged_at = models.DateTimeField(
        default=timezone.now,
        help_text="The time when the error was logged."
    )
    messages = models.JSONField(
        default=list,
        blank=True,
        null=True,
        help_text="List of output message codes."
    )
    data = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Additional data related to the error."
    )

    class Meta:
        """Meta class for DCASErrorLog."""

        db_table = 'dcas_error_log'
        verbose_name = _('Error Log')
        ordering = ['-logged_at']

    @classmethod
    def export_resource_classes(cls):
        """Export resource classes for import-export."""
        from dcas.resources import DCASErrorLogResource

        return {
            "DCASErrorLog": ("DCASErrorLog Resource", DCASErrorLogResource)
        }


class DCASErrorLogOutputFile(models.Model):
    """Class for DCASErrorLog output file."""

    request = models.ForeignKey(
        DCASRequest, on_delete=models.CASCADE,
        related_name='error_log_output_files',
        help_text="The DCAS request associated with this error."
    )
    file_name = models.CharField(
        max_length=255,
        null=True,
        blank=True
    )
    status = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING,
        help_text="The delivery status of the file."
    )
    path = models.TextField(
        null=True,
        blank=True,
        help_text="Full path to the uploaded file."
    )
    size = models.PositiveBigIntegerField(default=0)
    submitted_at = models.DateTimeField(
        default=timezone.now,
        null=True,
        blank=True,
        help_text="The time when the file was submitted."
    )
    started_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="The time when the file processing started."
    )
    finished_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="The time when the file processing finished."
    )

    @property
    def file_exists(self):
        """Check if the file exists in the storage."""
        from dcas.utils import dcas_output_file_exists
        if not self.path:
            return False
        return dcas_output_file_exists(self.path, 'OBJECT_STORAGE')

    class Meta:
        """Meta class for DCASErrorLogOutputFile."""

        verbose_name = _('Error log file')
        ordering = ['-submitted_at']


@receiver(post_delete, sender=DCASErrorLogOutputFile)
def post_delete_dcas_error_output_file(sender, instance, **kwargs):
    """Delete csv file in s3 object storage."""
    from dcas.utils import remove_dcas_output_file
    if instance.path:
        remove_dcas_output_file(instance.path, 'OBJECT_STORAGE')
