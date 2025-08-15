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


def decimal_to_dms(lat, lon):
    """Convert decimal coordinates to DMS format."""
    def convert(value, is_lat):
        """Convert a decimal value to DMS format."""
        # Get hemisphere
        if is_lat:
            hemisphere = 'N' if value >= 0 else 'S'
        else:
            hemisphere = 'E' if value >= 0 else 'W'

        value = abs(value)
        degrees = int(value)
        minutes_float = (value - degrees) * 60
        minutes = int(minutes_float)
        seconds = round((minutes_float - minutes) * 60, 2)

        return f"{degrees}°{minutes}'{seconds}\"{hemisphere}"

    lat_dms = convert(lat, is_lat=True)
    lon_dms = convert(lon, is_lat=False)
    return lat_dms, lon_dms
