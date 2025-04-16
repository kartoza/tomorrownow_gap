# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Message
"""

from django.contrib.gis.db import models

from dcas.models.config import DCASConfig


class DCASMessagePriority(models.Model):
    """Model that represents message priority."""

    config = models.ForeignKey(
        DCASConfig, on_delete=models.CASCADE
    )
    code = models.CharField(max_length=50)
    priority = models.IntegerField()

    def __str__(self):
        return f'{self.code}'

    class Meta:
        """Meta class for DCASMessagePriority."""

        indexes = [
            models.Index(fields=['code']),
            models.Index(fields=['priority']),
        ]
