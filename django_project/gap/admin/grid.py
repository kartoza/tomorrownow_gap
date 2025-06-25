# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""

from django.contrib import admin

from gap.models import Grid, GridSet


@admin.register(Grid)
class GridAdmin(admin.ModelAdmin):
    """Admin for Grid."""

    list_display = (
        'unique_id', 'latitude', 'longitude',
        'elevation', 'name', 'country'
    )

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
