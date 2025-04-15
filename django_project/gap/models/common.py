# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point, Polygon

from core.models.common import Definition


class Country(Definition):
    """Model representing a country.

    Attributes:
        name (str): Name of the country.
        iso_a3 (str): ISO A3 country code, unique.
        geometry (Polygon):
            MultiPolygonField geometry representing the country boundaries.
    """

    iso_a3 = models.CharField(
        unique=True,
        max_length=255
    )
    geometry = models.MultiPolygonField(
        srid=4326,
        blank=True,
        null=True
    )

    class Meta:  # noqa
        verbose_name_plural = 'countries'
        ordering = ['name']

    @staticmethod
    def get_countries_by_point(point: Point):
        """Get country by point."""
        return Country.objects.filter(
            geometry__intersects=point
        )

    @staticmethod
    def get_countries_by_polygon(polygon: Polygon):
        """Get country by polygon."""
        return Country.objects.filter(
            geometry__intersects=polygon
        )


class Provider(Definition):
    """Model representing a data provider."""

    pass


class Unit(Definition):
    """Model representing an unit of a measurement."""

    pass


class Village(Definition):
    """Model representing village."""

    pass


class County(Definition):
    """Model representing a county."""

    pass


class SubCounty(Definition):
    """Model representing a sub-county."""

    pass


class Ward(Definition):
    """Model representing a ward."""

    pass


class Language(Definition):
    """Model representing a language."""

    code = models.CharField(
        max_length=10,
        unique=True,
        help_text='Language code',
        null=True,
        blank=True
    )
