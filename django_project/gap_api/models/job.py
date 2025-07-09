# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for Job submitted by user
"""

import os
import uuid
import time
import json
from redis import Redis
from django.core.cache import cache
from django.db import models
from django.conf import settings
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.core.serializers.json import DjangoJSONEncoder

from core.models.background_task import TaskStatus
from gap_api.models.user_file import UserFile


class JobType(models.TextChoices):
    """Job type choices."""

    DATA_REQUEST = 'DataRequest', _('Data Request')


class Job(models.Model):
    """Model represents job submitted by user."""

    MAX_REDIS_KEY_EXPIRY = 60 * 60 * 1  # 1 hour

    uuid = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique identifier for the analysis."
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        help_text=_('User who submitted the job.')
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
    task_id = models.CharField(
        max_length=512,
        null=True,
        blank=True,
        help_text=_('ID of the task in the background processing system.')
    )
    size = models.PositiveBigIntegerField(default=0)

    @property
    def is_async(self):
        """Check if the job is asynchronous."""
        return self.queue_name is not None and self.queue_name != ''

    def set_user_file(self, user_file: UserFile):
        """Set the output user file for the job."""
        self.output_file = user_file
        self.size = user_file.size if user_file else 0
        if user_file is None:
            self.errors = 'No results from given query parameters.'
        self.save(update_fields=['output_file', 'size', 'errors'])

    @property
    def cache_key(self):
        """Return cache key for this job."""
        return f'job:{self.uuid}'

    @property
    def cache_payload(self):
        """Return cache payload for this job."""
        url = None
        content_type = None
        file_name = None
        if self.status == TaskStatus.COMPLETED:
            if self.output_file:
                url = self.output_file.generate_url()
                file_name = os.path.basename(self.output_file.name)
                content_type = (
                    'application/x-netcdf' if file_name.endswith('.nc') else
                    'text/csv'
                )
            elif self.output_json:
                content_type = 'application/json'
        return {
            'status': self.status,
            'errors': self.errors,
            'url': url,
            'output_json': self.output_json,
            'content_type': content_type,
            'file_name': file_name,
            'updated_on': int(time.time())
        }

    def save(self, *args, **kwargs):
        """Override the save method to handle Redis cache."""
        super().save(*args, **kwargs)

        # store job in Redis cache
        redis_client: Redis = cache._cache.get_client()
        redis_client.set(
            self.cache_key,
            json.dumps(self.cache_payload, cls=DjangoJSONEncoder),
            ex=self.MAX_REDIS_KEY_EXPIRY
        )
