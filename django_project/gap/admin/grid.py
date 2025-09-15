# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""

from django.contrib import admin

from gap.models import Grid, GridSet, Country


@admin.action(description='Update Turkana Grids')
def update_turkana_grids(modeladmin, request, queryset):
    """Update Turkana Grids."""
    grids = Grid.objects.filter(
        name='Turkana',
        country__isnull=True
    )
    modeladmin.message_user(
        request,
        f'Found {grids.count()} Turkana grids without country.'
    )
    # update country to Kenya
    country = Country.objects.get(name='Kenya')
    grids.update(country=country)


@admin.register(Grid)
class GridAdmin(admin.ModelAdmin):
    """Admin for Grid."""

    list_display = (
        'unique_id', 'latitude', 'longitude',
        'elevation', 'name', 'country'
    )
    list_filter = ['country']
    search_fields = ['unique_id', 'name']
    actions = [update_turkana_grids]

    def latitude(self, obj: Grid):
        """Latitude of Grid."""
        return obj.geometry.centroid.y

    def longitude(self, obj: Grid):
        """Longitude of Grid."""
        return obj.geometry.centroid.x


@admin.register(GridSet)
class GridSetAdmin(admin.ModelAdmin):
    """Admin for GridSet."""

    list_display = (
        'country', 'resolution'
    )
