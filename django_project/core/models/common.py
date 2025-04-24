# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: General models
"""

from django.db import models


class Definition(models.Model):
    """Abstract model for Model that has name and description.

    Attributes:
        name (str): Name of object.
        description (str): Description of object.
    """

    name = models.CharField(
        max_length=512
    )
    description = models.TextField(
        null=True, blank=True
    )

    def __str__(self):
        return self.name

    class Meta:  # noqa: D106
        abstract = True
        ordering = ['name']

    def normalize_name(self):
        """Normalize name to lowercase and remove spaces."""
        return self.name.replace(' ', '_').replace('-', '_')
