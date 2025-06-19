"""Tomorrow Now GAP."""

from django.contrib import admin
from frontend.models import PagePermission


@admin.register(PagePermission)
class PagePermissionAdmin(admin.ModelAdmin):
    """Admin for PagePermission model."""
    list_display = ("page",)
    filter_horizontal = ("groups",)
