# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Config
"""

from django.contrib.gis.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from core.models.background_task import TaskStatus
from gap.models.common import Country


class DCASRequest(models.Model):
    """Model representing a request to generate DCAS data."""

    requested_at = models.DateTimeField(
        default=timezone.now,
        help_text="The time when the request was created."
    )
    country = models.ForeignKey(
        Country,
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )
    start_time = models.DateTimeField(
        null=True,
        blank=True
    )
    end_time = models.DateTimeField(
        null=True,
        blank=True
    )
    status = models.CharField(
        max_length=255,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING,
        help_text="The status of the process."
    )
    progress_text = models.TextField(
        null=True,
        blank=True,
        help_text="Progress or note from the pipeline."
    )
    config = models.JSONField(blank=True, default=dict, null=True)

    class Meta:  # noqa
        """Meta class for DCASRequest."""

        db_table = 'dcas_request'
        verbose_name = _('Request')
        ordering = ['-requested_at']
