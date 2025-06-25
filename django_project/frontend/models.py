# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Page Permissions

"""

from django.db import models
from django.contrib.auth.models import Group
from knox.models import AuthToken


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

    @classmethod
    def get_page_permissions(cls, user):
        """Get page permissions for a user."""
        if user.is_superuser:
            # Superusers have access to all pages
            pages = (
                PagePermission.objects
                .values_list('page', flat=True)
                .distinct()
            )
        else:
            pages = (
                PagePermission.objects
                .filter(groups__in=user.groups.all())
                .values_list('page', flat=True)
                .distinct()
            )
        return list(pages)


class APIKeyMetadata(models.Model):
    """Metadata for API keys."""

    token = models.OneToOneField(
        AuthToken,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="metadata"
    )
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)

    def __str__(self):
        return f"{self.name} ({self.pk})"
