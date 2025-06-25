# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Grid Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point

from core.models.common import Definition
from gap.models.common import Country


class Grid(Definition):
    """Model representing a grid system."""

    unique_id = models.CharField(
        unique=True,
        max_length=255
    )
    geometry = models.PolygonField(
        srid=4326
    )
    elevation = models.FloatField()
    country = models.ForeignKey(
        Country, on_delete=models.SET_NULL,
        null=True, blank=True
    )

    @staticmethod
    def get_grids_by_point(point: Point):
        """Get grids by point."""
        return Grid.objects.filter(
            geometry__intersects=point
        )


class GridSet(Definition):
    """Model representing a set of grids."""

    country = models.ForeignKey(
        Country, on_delete=models.CASCADE
    )
    resolution = models.CharField(
        max_length=50,
        help_text="Resolution of the grid set, e.g., '1km', '10km'."
    )
    metadata = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Additional metadata for the grid set."
    )
    config = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Configuration settings for the grid set."
    )
