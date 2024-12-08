# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for User File
"""

from django.db import models
from django.conf import settings
import hashlib
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.db.models.signals import post_delete
from django.dispatch import receiver


class UserFile(models.Model):
    """Model to store user generated file from API request."""

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE
    )
    name = models.CharField(
        max_length=512,
        help_text='File path to the storage.'
    )
    created_on = models.DateTimeField(
        auto_now_add=True
    )
    query_params = models.JSONField(
        default=dict,
        help_text='Query parameters that generate the file.'
    )
    query_hash = models.CharField(
        max_length=512,
        help_text='Hash that can be used to cache the query result.',
        editable=False,
        blank=True
    )
    size = models.IntegerField(default=0)

    def _calculate_hash(self):
        """Calculate hash from query params."""
        combined_str = ""

        # add product
        combined_str += f'product:{self.query_params.get("product")}'
        # add attributes
        attributes = self.query_params.get("attributes")
        combined_str += f'product:{",".join(attributes)}'
        # add start_date
        combined_str += f'start_date:{self.query_params.get("start_date")}'
        # add start_time
        combined_str += (
            f'start_time:{self.query_params.get("start_time", "-")}'
        )
        # add end_date
        combined_str += f'end_date:{self.query_params.get("end_date")}'
        # add end_time
        combined_str += f'end_time:{self.query_params.get("end_time", "-")}'
        # add output_type
        combined_str += f'output_type:{self.query_params.get("output_type")}'
        # add query geom type
        combined_str += f'geom_type:{self.query_params.get("geom_type")}'
        # add wkt geom
        combined_str += f'geometry:{self.query_params.get("geometry")}'
        # add altitudes
        combined_str += f'altitudes:{self.query_params.get("altitudes", "-")}'

        return hashlib.sha512(combined_str.encode()).hexdigest()

    def save(self, *args, **kwargs):
        """Override the save method to calculate and set the hash."""
        if not self.query_hash:
            self.query_hash = self._calculate_hash()
        super().save(*args, **kwargs)

    def generate_url(self):
        """Generate pre-signed url to the storage."""
        s3_storage: S3Boto3Storage = storages["gap_products"]
        return s3_storage.url(self.name)

    def find_in_cache(self):
        """Find UserFile object in cache."""
        if not self.query_hash:
            self.query_hash = self._calculate_hash()

        return UserFile.objects.filter(
            query_hash=self.query_hash
        ).order_by('-created_on').first()


@receiver(post_delete, sender=UserFile)
def post_delete_user_file(sender, instance, **kwargs):
    """Delete user file in s3 object storage."""
    s3_storage: S3Boto3Storage = storages["gap_products"]
    if s3_storage.exists(instance.name):
        s3_storage.delete(instance.name)
