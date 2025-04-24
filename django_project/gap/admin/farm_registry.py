# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import FarmRegistryGroup, FarmRegistry


@admin.register(FarmRegistryGroup)
class FarmRegistryGroupAdmin(AbstractDefinitionAdmin):
    """FarmRegistryGroup admin."""

    list_display = (
        'name', 'date_time', 'is_latest', 'country'
    )


@admin.register(FarmRegistry)
class FarmRegistryAdmin(admin.ModelAdmin):
    """FarmRegistry admin."""

    list_display = (
        'farm', 'crop', 'crop_stage_type', 'planting_date',
        'crop_growth_stage', 'growth_stage_start_date', 'group'
    )
    list_filter = (
        'crop', 'crop_stage_type', 'group'
    )
    readonly_fields = (
        'farm',
    )
