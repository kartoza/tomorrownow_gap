# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Geometry utils.
"""

from django.db.models import FloatField
from django.contrib.gis.db.models.functions import GeoFunc
from django.contrib.gis.geos import Polygon


class ST_X(GeoFunc):
    """Custom GeoFunc to extract lon."""

    output_field = FloatField()
    function = 'ST_X'


class ST_Y(GeoFunc):
    """Custom GeoFunc to extract lat."""

    output_field = FloatField()
    function = 'ST_Y'


def split_polygon_to_bbox(polygon: Polygon, size: int):
    """Split a polygon into smaller bounding box.

    :param Polygon polygon: Polygon input that will be split.
    :param int size:
        BBOX size that will be used to split the polygon in meters.
    """
    source_crs = polygon.crs

    if not source_crs:
        raise ValueError('Source CRS not provided on polygon.')

    source_srid = source_crs.srid
    used_srid = 3857
    if source_crs != used_srid:
        polygon = polygon.transform(used_srid, clone=True)

    output = []
    min_x, min_y, max_x, max_y = tuple(int(coord) for coord in polygon.extent)
    x = min_x
    while x < max_x:
        y = min_y
        while y < max_y:
            new_x = x + size
            new_y = y + size

            if new_x > max_x:
                new_x = max_x
            if new_y > max_y:
                new_y = max_y

            bbox = Polygon.from_bbox((x, y, new_x, new_y))
            bbox.srid = used_srid
            if polygon.overlaps(bbox) or polygon.contains(bbox):
                output.append(bbox)
            y += size
        x += size

    if source_srid != used_srid:
        return [bbox.transform(source_srid, clone=True) for bbox in output]
    else:
        return output
