# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Admin for DCAS Models
"""

from import_export.admin import ExportMixin
from import_export_celery.admin_actions import create_export_job_action
from django.contrib import admin, messages
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from django.utils.translation import gettext_lazy as _

from gap.models import Country
from dcas.models import (
    DCASConfig,
    DCASConfigCountry,
    DCASRule,
    DCASRequest,
    DCASOutput,
    DCASErrorLog,
    GDDConfig,
    GDDMatrix,
    DCASMessagePriority,
    DCASDownloadLog,
    DCASPermissionType,
    DCASCountryUserObjectPermission,
    DCASCountryGroupObjectPermission
)
from dcas.resources import DCASErrorLogResource
from core.utils.file import format_size
from dcas.tasks import (
    run_dcas,
    export_dcas_minio,
    export_dcas_sftp,
    log_dcas_error,
    clear_all_dcas_error_logs
)


class ConfigByCountryInline(admin.TabularInline):
    """Inline list for config by country."""

    model = DCASConfigCountry
    extra = 0


@admin.register(DCASConfig)
class DCASConfigAdmin(admin.ModelAdmin):
    """Admin page for DCASConfig."""

    list_display = ('name', 'description', 'is_default')
    inlines = (ConfigByCountryInline,)


@admin.register(DCASRule)
class DCASRuleAdmin(admin.ModelAdmin):
    """Admin page for DCASRule."""

    list_display = (
        'crop', 'crop_stage_type', 'crop_growth_stage',
        'parameter', 'min_range', 'max_range', 'code'
    )
    list_filter = (
        'crop', 'crop_stage_type', 'crop_growth_stage',
        'parameter'
    )


@admin.action(description='Trigger DCAS processing')
def trigger_dcas_processing(modeladmin, request, queryset):
    """Trigger dcas processing."""
    run_dcas.delay(queryset.first().id)
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.action(description='Send DCAS output to minio')
def trigger_dcas_output_to_minio(modeladmin, request, queryset):
    """Send DCAS output to minio."""
    export_dcas_minio.delay(queryset.first().id)
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.action(description='Send DCAS output to sftp')
def trigger_dcas_output_to_sftp(modeladmin, request, queryset):
    """Send DCAS output to sftp."""
    export_dcas_sftp.delay(queryset.first().id)
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.action(description='Trigger DCAS error handling')
def trigger_dcas_error_handling(modeladmin, request, queryset):
    """Trigger DCAS error handling."""
    log_dcas_error.delay(queryset.first().id)
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.register(DCASRequest)
class DCASRequestAdmin(admin.ModelAdmin):
    """Admin page for DCASRequest."""

    list_display = ('requested_at', 'start_time', 'end_time', 'status')
    list_filter = ('country',)
    actions = (
        trigger_dcas_processing,
        trigger_dcas_output_to_minio,
        trigger_dcas_output_to_sftp,
        trigger_dcas_error_handling
    )


@admin.register(DCASOutput)
class DCASOutputAdmin(admin.ModelAdmin):
    """Admin page for DCASOutput."""

    list_display = (
        'delivered_at', 'request',
        'file_name', 'status',
        'get_size', 'delivery_by', 'file_exists')
    list_filter = ('request', 'status', 'delivery_by')

    def get_size(self, obj: DCASOutput):
        """Get the size."""
        return format_size(obj.size)

    get_size.short_description = 'Size'
    get_size.admin_order_field = 'size'


@admin.action(description='Clear all DCAS error logs')
def run_clear_all_dcas_error_logs(modeladmin, request, queryset):
    """Clear all DCAS error logs."""
    clear_all_dcas_error_logs.delay()
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.register(DCASErrorLog)
class DCASErrorLogAdmin(ExportMixin, admin.ModelAdmin):
    """Admin class for DCASErrorLog model."""

    resource_class = DCASErrorLogResource
    actions = [create_export_job_action, run_clear_all_dcas_error_logs]

    list_display = (
        "id",
        "request",
        "get_farm_unique_id",
        "error_type",
        "error_message",
        "logged_at",
    )

    search_fields = (
        "error_message", "farm_registry__farm__unique_id",
        "request__id"
    )
    list_filter = ("error_type", "logged_at", "request_id")
    readonly_fields = ("request", "farm_registry",)

    def get_farm_unique_id(self, obj: DCASErrorLog):
        """Get the farm unique ID."""
        return obj.farm_registry.farm.unique_id

    get_farm_unique_id.short_description = 'Farm ID'
    get_farm_unique_id.admin_order_field = 'farm_registry__farm__unique_id'

# GDD Config and Matrix


@admin.register(GDDConfig)
class GDDConfigAdmin(admin.ModelAdmin):
    """Admin interface for GDDConfig."""

    list_display = ('crop', 'base_temperature', 'cap_temperature', 'config')
    list_filter = ('config', 'crop')


@admin.register(GDDMatrix)
class GDDMatrixAdmin(admin.ModelAdmin):
    """Admin interface for GDDMatrix."""

    list_display = ('crop', 'crop_stage_type', 'gdd_threshold', 'config')
    list_filter = ('crop', 'crop_stage_type', 'config')


@admin.register(DCASMessagePriority)
class DCASMessagePriorityAdmin(admin.ModelAdmin):
    """Admin interface for DCASMessagePriority."""

    list_display = ('code', 'priority', 'config')
    list_filter = ('config',)


@admin.register(DCASDownloadLog)
class DCASDownloadLogAdmin(admin.ModelAdmin):
    """Admin interface for DCASDownloadLog."""

    list_display = ("output", "user", "requested_at")
    list_filter = ("user",)
    search_fields = ("output__file_name", "user__email")


def _view_dcas_output_permission():
    """Fetch permission to view DCAS output."""
    permission = Permission.objects.filter(
        codename=DCASPermissionType.VIEW_DCAS_OUTPUT_COUNTRY
    ).first()
    if permission is None:
        # create a new permission object if it doesn't exist
        permission = Permission.objects.create(
            codename=DCASPermissionType.VIEW_DCAS_OUTPUT_COUNTRY,
            name=_("View DCAS Output Country"),
            content_type=ContentType.objects.get_for_model(Country)
        )
    return permission


@admin.register(DCASCountryGroupObjectPermission)
class DCASCountryGroupObjectPermissionAdmin(admin.ModelAdmin):
    """DCASCountryGroupObjectPermission admin."""

    list_display = ('group', 'content_object',)

    def get_form(self, request, obj=None, **kwargs):
        """Override the permission dropdown."""
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['permission'].disabled = True
        form.base_fields['permission'].initial = (
            _view_dcas_output_permission()
        )
        return form


@admin.register(DCASCountryUserObjectPermission)
class DCASCountryUserObjectPermissionAdmin(admin.ModelAdmin):
    """DCASCountryUserObjectPermission admin."""

    list_display = ('user', 'content_object',)

    def get_form(self, request, obj=None, **kwargs):
        """Override the permission dropdown."""
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['permission'].disabled = True
        form.base_fields['permission'].initial = (
            _view_dcas_output_permission()
        )
        return form
