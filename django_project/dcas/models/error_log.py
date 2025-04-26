# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Output
"""

from django.utils import timezone
from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

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
