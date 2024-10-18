# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Airborne Observation Data Reader.
        AirBorne dataset is the station that is always moving.
"""

from datetime import datetime

from django.contrib.gis.db.models.functions import Distance
from django.contrib.gis.geos import Polygon, Point

from gap.models import Measurement, StationHistory
from gap.providers.observation import ObservationDatasetReader


class ObservationAirborneDatasetReader(ObservationDatasetReader):
    """Class to read observation airborne observation data."""

    def _find_nearest_station_by_point(self, point: Point = None):
        p = point
        if p is None:
            p = self.location_input.point
        qs = StationHistory.objects.annotate(
            distance=Distance('geometry', p)
        ).filter(
            station__provider=self.dataset.provider
        )

        qs = self.query_by_altitude(qs)
        qs = qs.order_by('distance').first()
        if qs is None:
            return None
        return [qs]

    def _find_nearest_station_by_bbox(self):
        points = self.location_input.points
        polygon = Polygon.from_bbox(
            (points[0].x, points[0].y, points[1].x, points[1].y)
        )
        qs = StationHistory.objects.filter(
            geometry__intersects=polygon
        ).filter(
            station__provider=self.dataset.provider
        )

        qs = self.query_by_altitude(qs)
        qs = qs.order_by('id')
        if not qs.exists():
            return None
        return qs

    def _find_nearest_station_by_polygon(self):
        qs = StationHistory.objects.filter(
            geometry__within=self.location_input.polygon
        ).filter(
            station__provider=self.dataset.provider
        )

        qs = self.query_by_altitude(qs)
        qs = qs.order_by('id')
        if not qs.exists():
            return None
        return qs

    def _find_nearest_station_by_points(self):
        points = self.location_input.points
        results = {}
        for point in points:
            rs = self._find_nearest_station_by_point(point)
            if rs is None:
                continue
            if rs[0].id in results:
                continue
            results[rs[0].id] = rs[0]
        return results.values()

    def get_measurements(self, start_date: datetime, end_date: datetime):
        """Return measurements."""
        nearest_histories = self.get_nearest_stations()
        if nearest_histories is None:
            return None

        return Measurement.objects.select_related(
            'dataset_attribute', 'dataset_attribute__attribute',
            'station'
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station_history__in=nearest_histories
        ).order_by('date_time', 'station', 'dataset_attribute')
