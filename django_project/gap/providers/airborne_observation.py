# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Airborne Observation Data Reader.
        AirBorne dataset is the station that is always moving.
"""

from datetime import datetime
from typing import List, Tuple

from django.db.models import F, QuerySet
from django.contrib.gis.db.models.functions import Distance
from django.contrib.gis.geos import Polygon, Point

from gap.models import (
    Measurement, StationHistory, Dataset, DatasetAttribute
)
from gap.providers.base import BaseReaderBuilder
from gap.providers.observation import (
    ObservationDatasetReader,
    ObservationParquetReader
)
from gap.utils.reader import DatasetReaderInput


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
        if isinstance(nearest_histories, QuerySet):
            nearest_histories = nearest_histories.filter(
                date_time__gte=start_date,
                date_time__lte=end_date
            )
        if (
            nearest_histories is None or
            self._get_count(nearest_histories) == 0
        ):
            return Measurement.objects.none()

        return Measurement.objects.annotate(
            geom=F('station_history__geometry'),
            alt=F('station_history__altitude')
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station_history__in=nearest_histories
        ).order_by('date_time')


class ObservationAirborneParquetReader(
    ObservationParquetReader, ObservationAirborneDatasetReader
):
    """Class for parquet reader for Airborne dataset."""

    has_month_partition = True
    has_altitudes = True
    station_id_key = 'st_hist_id'


class ObservationAirborneReaderBuilder(BaseReaderBuilder):
    """Class to build airborne observation reader."""

    def __init__(
        self, dataset: Dataset, attributes: List[DatasetAttribute],
        location_input: DatasetReaderInput,
        start_date: datetime, end_date: datetime,
        altitudes: Tuple[float, float] = None, use_parquet=False
    ):
        """Initialize ObservationAirborneReaderBuilder class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date
        )
        self.altitudes = altitudes
        self.use_parquet = use_parquet

    def build(self) -> ObservationAirborneDatasetReader:
        """Build a new Dataset Reader."""
        if self.use_parquet:
            return ObservationAirborneParquetReader(
                self.dataset, self.attributes,
                self.location_input, self.start_date,
                self.end_date, altitudes=self.altitudes
            )

        return ObservationAirborneDatasetReader(
            self.dataset, self.attributes,
            self.location_input, self.start_date,
            self.end_date, altitudes=self.altitudes
        )
