# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Admin for DCAS Models
"""

from import_export.admin import ExportMixin
from import_export_celery.admin_actions import create_export_job_action
from django.contrib import admin, messages

from dcas.models import (
    DCASConfig,
    DCASConfigCountry,
    DCASRule,
    DCASRequest,
    DCASOutput,
    DCASErrorLog,
    GDDConfig,
    GDDMatrix
)
from dcas.resources import DCASErrorLogResource
from core.utils.file import format_size
from dcas.tasks import (
    run_dcas,
    export_dcas_minio,
    export_dcas_sftp,
    log_farms_without_messages
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
    log_farms_without_messages.delay(queryset.first().id)
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
        'get_size', 'delivery_by')
    list_filter = ('request', 'status', 'delivery_by')

    def get_size(self, obj: DCASOutput):
        """Get the size."""
        return format_size(obj.size)

    get_size.short_description = 'Size'
    get_size.admin_order_field = 'size'


@admin.register(DCASErrorLog)
class DCASErrorLogAdmin(ExportMixin, admin.ModelAdmin):
    """Admin class for DCASErrorLog model."""

    resource_class = DCASErrorLogResource
    actions = [create_export_job_action]

    list_display = (
        "id",
        "request_id",
        "get_farm_unique_id",
        "error_type",
        "error_message",
        "logged_at",
    )

    search_fields = ("error_message", "farm__unique_id", "request__id")
    list_filter = ("error_type", "logged_at", "request_id")

    def get_farm_unique_id(self, obj: DCASErrorLog):
        """Get the farm unique ID."""
        return obj.farm.unique_id

    get_farm_unique_id.short_description = 'Farm ID'
    get_farm_unique_id.admin_order_field = 'farm__unique_id'

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
