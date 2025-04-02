# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Output
"""

from django.utils import timezone
from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

from core.models.background_task import TaskStatus
from dcas.models.request import DCASRequest


class DCASDeliveryMethod(models.TextChoices):
    """Delivery method choices."""

    SFTP = 'SFTP', _('SFTP')
    OBJECT_STORAGE = 'OBJECT_STORAGE', _('OBJECT_STORAGE')


class DCASOutput(models.Model):
    """Model to track the delivery of file output to SFTP."""

    request = models.ForeignKey(
        DCASRequest, on_delete=models.CASCADE,
        related_name='output',
        help_text="The DCAS request associated with this output."
    )
    file_name = models.CharField(
        max_length=255,
        null=True,
        blank=True
    )
    delivered_at = models.DateTimeField(
        default=timezone.now,
        null=True,
        blank=True,
        help_text="The time when the file was delivered."
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
    delivery_by = models.CharField(
        null=True,
        blank=True,
        max_length=255,
        choices=DCASDeliveryMethod.choices,
        default=DCASDeliveryMethod.SFTP,
        help_text="The type of delivery."
    )
    size = models.PositiveBigIntegerField(default=0)

    class Meta:
        """Meta class for DCASOutput."""

        db_table = 'dcas_output'
        verbose_name = _('Output')
        ordering = ['-delivered_at']
