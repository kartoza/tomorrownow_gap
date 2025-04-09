# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin, messages

from spw.models import (
    RModel, RModelOutput, RModelExecutionLog,
    SPWOutput, SPWErrorLog
)
from spw.tasks import start_plumber_process


@admin.action(description='Restart plumber process')
def restart_plumber_process(modeladmin, request, queryset):
    """Restart plumber process action."""
    start_plumber_process.apply_async(queue='plumber')
    modeladmin.message_user(
        request,
        'Plumber process will be started in background!',
        messages.SUCCESS
    )


class RModelOutputInline(admin.TabularInline):
    """Inline list for model output in RModel admin page."""

    model = RModelOutput
    extra = 1


@admin.register(RModel)
class RModelAdmin(admin.ModelAdmin):
    """Admin page for RModel."""

    list_display = ('name', 'version', 'created_on')
    inlines = [RModelOutputInline]
    actions = [restart_plumber_process]


@admin.register(RModelExecutionLog)
class RModelExecutionLogAdmin(admin.ModelAdmin):
    """Admin page for RModelExecutionLog."""

    list_display = (
        'model', 'start_date_time', 'status', 'total_time'
    )

    def total_time(self, obj):
        """Calculate total time in seconds."""
        if obj.start_date_time and obj.end_date_time:
            delta = obj.end_date_time - obj.start_date_time
            return delta.total_seconds()
        return None

    total_time.short_description = 'Total Time (s)'


@admin.register(SPWOutput)
class SPWOutputAdmin(admin.ModelAdmin):
    """Admin page for SPWOutput."""

    list_display = (
        'identifier', 'tier', 'plant_now_string', 'description'
    )


@admin.register(SPWErrorLog)
class SPWErrorLogAdmin(admin.ModelAdmin):
    """Admin page for SPWErrorLog."""

    list_display = (
        'farm', 'generated_date', 'farm_group', 'grid_unique_id'
    )
    readonly_fields = (
        'farm', 'farm_group', 'generated_date', 'grid_unique_id',
    )
