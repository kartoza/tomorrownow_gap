# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Page Permissions

"""

from django.db import models
from django.contrib.auth.models import Group


class PagePermission(models.Model):
    """Grant access to groups."""

    page = models.CharField(
        max_length=100,
        unique=True,
        help_text="A short, unique identifier for the page (e.g. 'dcas_csv')."
    )
    groups = models.ManyToManyField(
        Group,
        related_name="page_permissions",
        help_text="Which auth groups are allowed to view this page."
    )

    class Meta:
        """Meta options for PagePermission model."""

        db_table = "frontend_page_permission"
        verbose_name = "Page Permission"
        verbose_name_plural = "Page Permissions"

    def __str__(self):
        return (
            f"{self.page} - {', '.join(
                g.name for g in self.groups.all()
            )}"
        )
