"""Tomorrow Now GAP."""

from django.contrib import admin
from frontend.models import PagePermission, APIKeyMetadata


@admin.register(PagePermission)
class PagePermissionAdmin(admin.ModelAdmin):
    """Admin for PagePermission model."""

    list_display = ("page",)
    filter_horizontal = ("groups",)


@admin.register(APIKeyMetadata)
class APIKeyMetadataAdmin(admin.ModelAdmin):
    """Admin for APIKeyMetadata model."""

    list_display = ("token_id", "name", "description")
    search_fields = ("name", "description")
