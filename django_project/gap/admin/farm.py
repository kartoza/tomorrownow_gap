# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""

import json
from django.contrib import admin
from django.utils.html import format_html
from django.http import HttpResponse
from django.core.serializers.json import DjangoJSONEncoder

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm
)
from gap.models.farm_group import (
    FarmGroup, FarmGroupCropInsightField,
    FarmGroupMembership, FarmGroupRelationship
)
from gap.tasks.crop_insight import generate_crop_plan


class FarmGroupCropInsightFieldInline(admin.TabularInline):
    """Inline list for model output in FarmGroupCropInsightField."""

    model = FarmGroupCropInsightField
    extra = 0


@admin.action(description='Recreate fields')
def recreate_farm_group_fields(modeladmin, request, queryset):
    """Recreate farm group fields."""
    for group in queryset.all():
        group.prepare_fields()


@admin.action(description='Download fields as json')
def download_farm_group_fields(modeladmin, request, queryset):
    """Download farm group fields."""
    data = {}
    for group in queryset.all():
        fields = group.farmgroupcropinsightfield_set.all()
        fields_to_include = ['field', 'column_number', 'label', 'active']
        data[group.name] = list(fields.values(*fields_to_include))

    # Convert the data to JSON
    response_data = json.dumps(data, cls=DjangoJSONEncoder)

    # Create the HttpResponse with the correct content_type for JSON
    response = HttpResponse(response_data, content_type='application/json')
    response['Content-Disposition'] = 'attachment; filename=farm_groups.json'
    return response


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
    exclude = ('farms',)
    filter_horizontal = ('farms', 'users')
    inlines = (FarmGroupCropInsightFieldInline,)
    actions = (
        recreate_farm_group_fields, run_crop_insight,
        download_farm_group_fields)
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
    actions = (assign_farm_grid,)

    def latitude(self, obj: Farm):
        """Latitude of farm."""
        return obj.geometry.y

    def longitude(self, obj: Farm):
        """Longitude of farm."""
        return obj.geometry.x


@admin.register(FarmGroupMembership)
class FarmGroupMembershipAdmin(admin.ModelAdmin):
    """Admin for FarmGroupMembership."""

    # Note: when adding FarmGroupRelationship manually,
    # it's better to use this class since it's not loading all the farms
    list_display = ('id', 'farm_id', 'farmgroup_id',)
    search_fields = ('farm_id',)
    list_filter = ('farmgroup_id',)


@admin.register(FarmGroupRelationship)
class FarmGroupRelationshipAdmin(admin.ModelAdmin):
    """Admin for FarmGroupRelationship."""

    list_display = (
        'id', 'farm', 'farmgroup'
    )
    search_fields = ('farm__unique_id',)
    list_filter = ('farmgroup',)
