# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Prise admins
"""
from django.contrib import admin

from prise.models import (
    PriseMessage,
    PrisePest,
    PriseData,
    PriseDataByPest,
    PriseMessageSchedule
)


@admin.register(PriseMessage)
class PriseMessageAdmin(admin.ModelAdmin):
    """Admin page for PriseMessage."""

    list_display = ('pest', 'farm_group', 'message_count')
    filter_horizontal = ('messages',)

    def message_count(self, obj: PriseMessage):
        """Message count."""
        return obj.messages.count()


@admin.register(PrisePest)
class PrisePestAdmin(admin.ModelAdmin):
    """Admin page for PrisePest."""

    list_display = ('pest', 'variable_name')


class PriseDataByPestInline(admin.TabularInline):
    """Inline list for model output in PriseDataByPest."""

    model = PriseDataByPest
    extra = 0


@admin.register(PriseData)
class PriseDataAdmin(admin.ModelAdmin):
    """Admin page for PriseData."""

    list_display = ('farm', 'ingested_at', 'generated_at', 'data_type')
    list_filter = ('data_type',)
    inlines = (PriseDataByPestInline,)
    readonly_fields = ('farm',)


@admin.register(PriseMessageSchedule)
class PriseMessageScheduleAdmin(admin.ModelAdmin):
    """Admin page for PriseMessageSchedule."""

    list_display = (
        'group', 'day_occurrence_in_month', 'day_of_week',
        'schedule_date', 'priority', 'active'
    )
    list_filter = ('group', 'active')
