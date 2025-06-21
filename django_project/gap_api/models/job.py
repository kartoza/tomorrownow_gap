# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for Job submitted by user
"""

from django.db import models
from django.conf import settings
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from core.models.background_task import TaskStatus
from gap_api.models.user_file import UserFile


class JobType(models.TextChoices):
    """Job type choices."""

    DATA_REQUEST = 'DataRequest', _('Data Request')


class Job(models.Model):
    """Model represents job submitted by user."""

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
    )
    job_type = models.CharField(
        max_length=50,
        choices=JobType.choices,
        default=JobType.DATA_REQUEST
    )
    status = models.CharField(
        max_length=50,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING
    )
    submitted_on = models.DateTimeField(
        default=timezone.now
    )
    started_at = models.DateTimeField(
        null=True,
        blank=True
    )
    finished_at = models.DateTimeField(
        null=True,
        blank=True
    )
    errors = models.TextField(
        null=True,
        blank=True
    )
    parameters = models.JSONField(
        default=dict,
        null=True,
        blank=True
    )
    output_file = models.ForeignKey(
        UserFile,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='job_output_file'
    )
    output_json = models.JSONField(
        default=dict,
        null=True,
        blank=True,
        help_text=_('Output JSON data from the job.')
    )
    queue_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text=_('Name of the queue where the job is submitted.')
    )
    wait_type = models.IntegerField(
        choices=[
            (0, _('No wait')),
            (1, _('Wait for completion')),
            (2, _('Wait for completion asynchronously'))
        ],
        default=0,
        help_text=_('Type of wait for the job to complete.')
    )

    @property
    def is_async(self):
        """Check if the job is asynchronous."""
        return self.queue_name is not None and self.queue_name != ''
