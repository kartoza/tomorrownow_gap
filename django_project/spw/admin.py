# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin, messages

from spw.models import RModel, RModelOutput, RModelExecutionLog, SPWOutput
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

    list_display = ('model', 'start_date_time', 'status')


@admin.register(SPWOutput)
class SPWOutputAdmin(admin.ModelAdmin):
    """Admin page for SPWOutput."""

    list_display = (
        'spw_output_identifier', 'spw_tier', 'plant_now_string', 'description'
    )
