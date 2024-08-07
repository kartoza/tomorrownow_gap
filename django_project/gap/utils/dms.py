# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DMS Utils.
"""
import re

from django.contrib.gis.geos import Point


def dms_to_decimal(
        degrees: int, minutes: int, seconds: float, direction: str
):
    """Convert DMS coordinates to decimal degrees."""
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ['S', 'W']:
        decimal = -decimal
    return decimal


def dms_string_to_point(coord_str: str) -> Point:
    """Change dms string to lat/lon.

    :return: Point
    """
    pattern = re.compile(
        r'(?P<latitude>\d+°\d+\'\d+(\.\d+)?"[NS]) '
        r'(?P<longitude>\d+°\d+\'\d+(\.\d+)?"[EW])')

    match = pattern.search(coord_str)
    if not match:
        raise ValueError("Invalid dms format")

    latitude_str = match.group('latitude')
    longitude_str = match.group('longitude')

    def parse_dms(dms_str):
        """Extract DMS from a string."""
        degree, rest = dms_str.split('°')
        minutes, seconds = rest.split('\'')
        seconds, direction = seconds.split('"')
        return dms_to_decimal(
            int(degree), int(minutes), float(seconds), direction
        )

    latitude = parse_dms(latitude_str)
    longitude = parse_dms(longitude_str)
    return Point(longitude, latitude)
