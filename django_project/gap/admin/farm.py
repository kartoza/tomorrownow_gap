# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""

from django.contrib import admin, messages
from django.utils.html import format_html

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm
)
from gap.models.farm_group import FarmGroup, FarmGroupCropInsightField
from gap.tasks.crop_insight import generate_spw, generate_crop_plan


class FarmGroupCropInsightFieldInline(admin.TabularInline):
    """Inline list for model output in FarmGroupCropInsightField."""

    model = FarmGroupCropInsightField
    extra = 0


@admin.action(description='Recreate fields')
def recreate_farm_group_fields(modeladmin, request, queryset):
    """Recreate farm group fields."""
    for group in queryset.all():
        group.prepare_fields()


@admin.action(description='Run crop insight')
def run_crop_insight(modeladmin, request, queryset):
    """Run crop insight."""
    generate_crop_plan.delay()


@admin.register(FarmGroup)
class FarmGroupAdmin(AbstractDefinitionAdmin):
    """FarmGroup admin."""

    list_display = (
        'id', 'name', 'description', 'user_count', 'farm_count', 'phone_number'
    )

    filter_horizontal = ('farms', 'users')
    inlines = (FarmGroupCropInsightFieldInline,)
    actions = (recreate_farm_group_fields, run_crop_insight)
    readonly_fields = ('displayed_headers',)

    def farm_count(self, obj: FarmGroup):
        """Return farm count."""
        return obj.farms.count()

    def user_count(self, obj: FarmGroup):
        """Return user count."""
        return obj.users.count()

    def displayed_headers(self, obj: FarmGroup):
        """Display headers as a table."""
        columns = "".join(
            f'<td style="padding: 10px; border: 1px solid gray">{header}</td>'
            for header in obj.headers
        )
        return format_html(
            '<div style="width:1000px; overflow:auto;">'
            '   <table>'
            f"      <thead><tr>{columns}</tr>"
            '   </table>'
            '</div>'
        )

    displayed_headers.allow_tags = True


@admin.action(description='Generate farms spw')
def generate_farm_spw(modeladmin, request, queryset):
    """Generate Farms SPW."""
    generate_spw.delay(list(queryset.values_list('id', flat=True)))
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.action(description='Assign farm grid')
def assign_farm_grid(modeladmin, request, queryset):
    """Generate Farms SPW."""
    for farm in queryset.all():
        farm.assign_grid()


@admin.register(FarmCategory)
class FarmCategoryAdmin(AbstractDefinitionAdmin):
    """FarmCategory admin."""

    pass


@admin.register(FarmRSVPStatus)
class FarmRSVPStatusAdmin(AbstractDefinitionAdmin):
    """FarmRSVPStatus admin."""

    pass


@admin.register(Farm)
class FarmAdmin(admin.ModelAdmin):
    """Admin for Farm."""

    list_display = (
        'unique_id', 'latitude', 'longitude',
        'rsvp_status', 'category', 'crop', 'grid'
    )
    search_fields = ('unique_id',)
    filter = ('unique_id',)
    list_filter = ('rsvp_status', 'category', 'crop')
    actions = (generate_farm_spw, assign_farm_grid)

    def latitude(self, obj: Farm):
        """Latitude of farm."""
        return obj.geometry.y

    def longitude(self, obj: Farm):
        """Longitude of farm."""
        return obj.geometry.x
