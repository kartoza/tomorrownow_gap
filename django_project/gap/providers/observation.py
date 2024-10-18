# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Observation Data Reader
"""

from datetime import datetime
from typing import List

from django.contrib.gis.db.models.functions import Distance
from django.contrib.gis.geos import Polygon, Point
from rest_framework.exceptions import ValidationError

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement
)
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetTimelineValue,
    BaseDatasetReader,
    DatasetReaderValue
)


class CSVBuffer:
    """An object that implements the write method of file-like interface."""

    def write(self, value):
        """Return the string to write."""
        yield value


class ObservationReaderValue(DatasetReaderValue):
    """Class that convert Dataset to TimelineValues."""

    date_variable = 'date'

    def __init__(
            self, val: List[DatasetTimelineValue],
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            start_date: datetime,
            end_date: datetime) -> None:
        """Initialize ObservationReaderValue class.

        :param val: value that has been read
        :type val: List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(val, location_input, attributes)
        self.start_date = start_date
        self.end_date = end_date

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream.

        :param suffix: file extension, defaults to '.csv'
        :type suffix: str, optional
        :param separator: separator, defaults to ','
        :type separator: str, optional
        :yield: bytes of csv file
        :rtype: bytes
        """
        headers = [
            'date',
            'lat',
            'lon',
            'altitude'
        ]
        for attr in self.attributes:
            headers.append(attr.attribute.variable_name)
        yield bytes(','.join(headers) + '\n', 'utf-8')

        for val in self.values:
            data = [
                val.get_datetime_repr('%Y-%m-%d'),
                str(val.location.y),
                str(val.location.x),
                str(val.altitude) if val.altitude else '',
            ]
            for attr in self.attributes:
                var_name = attr.attribute.variable_name
                if var_name in val.values:
                    data.append(str(val.values[var_name]))
                else:
                    data.append('')
            yield bytes(','.join(data) + '\n', 'utf-8')

    def to_netcdf_stream(self):
        """Generate NetCDF.

        :raises ValidationError: Not supported for Dataset
        """
        raise ValidationError({
            'Invalid Request Parameter': (
                'Output format netcdf is not available '
                'for Observation Dataset!'
            )
        })


class ObservationDatasetReader(BaseDatasetReader):
    """Class to read observation ground observation data."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: (float, float) = None

    ) -> None:
        """Initialize ObservationDatasetReader class.

        :param dataset: Dataset from observation provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.results: List[DatasetTimelineValue] = []

    def query_by_altitude(self, qs):
        """Query by altitude."""
        altitudes = self.altitudes
        try:
            if altitudes[0] is not None and altitudes[1] is not None:
                qs = qs.filter(
                    altitude__gte=altitudes[0]
                ).filter(
                    altitude__lte=altitudes[1]
                )
        except (IndexError, TypeError):
            pass
        return qs

    def _find_nearest_station_by_point(self, point: Point = None):
        p = point
        if p is None:
            p = self.location_input.point
        qs = Station.objects.annotate(
            distance=Distance('geometry', p)
        ).filter(
            provider=self.dataset.provider
        ).order_by('distance').first()
        if qs is None:
            return None
        return [qs]

    def _find_nearest_station_by_bbox(self):
        points = self.location_input.points
        polygon = Polygon.from_bbox(
            (points[0].x, points[0].y, points[1].x, points[1].y))
        qs = Station.objects.filter(
            geometry__within=polygon
        ).order_by('id')
        if not qs.exists():
            return None
        return qs

    def _find_nearest_station_by_polygon(self):
        qs = Station.objects.filter(
            geometry__within=self.location_input.polygon
        ).order_by('id')
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

    def get_nearest_stations(self):
        """Return nearest stations."""
        nearest_stations = None
        if self.location_input.type == LocationInputType.POINT:
            nearest_stations = self._find_nearest_station_by_point()
        elif self.location_input.type == LocationInputType.POLYGON:
            nearest_stations = self._find_nearest_station_by_polygon()
        elif self.location_input.type == LocationInputType.LIST_OF_POINT:
            nearest_stations = self._find_nearest_station_by_points()
        elif self.location_input.type == LocationInputType.BBOX:
            nearest_stations = self._find_nearest_station_by_bbox()
        return nearest_stations

    def get_measurements(self, start_date: datetime, end_date: datetime):
        """Return measurements data."""
        nearest_stations = self.get_nearest_stations()
        if nearest_stations is None:
            return
        return Measurement.objects.select_related(
            'dataset_attribute', 'dataset_attribute__attribute',
            'station'
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station__in=nearest_stations
        ).order_by('date_time', 'station', 'dataset_attribute')

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        measurements = self.get_measurements(start_date, end_date)
        if measurements is None:
            return

        # final result, group by datetime
        self.results = []

        iter_dt = None
        iter_loc = None
        iter_alt = None
        # group by location and date_time
        dt_loc_val = {}
        for measurement in measurements:
            # if it has history, use history location
            station_history = measurement.station_history
            if station_history:
                measurement_loc = station_history.geometry
                measurement_alt = station_history.altitude
            else:
                measurement_loc = measurement.station.geometry
                measurement_alt = measurement.station.altitude

            if iter_dt is None:
                iter_dt = measurement.date_time
                iter_loc = measurement_loc
                iter_alt = measurement_alt
            elif (
                    iter_loc != measurement_loc or
                    iter_dt != measurement.date_time or
                    iter_alt != measurement_alt
            ):
                self.results.append(
                    DatasetTimelineValue(
                        iter_dt, dt_loc_val, iter_loc, iter_alt
                    )
                )
                iter_dt = measurement.date_time
                iter_loc = measurement_loc
                iter_alt = measurement_alt
                dt_loc_val = {}
            dt_loc_val[
                measurement.dataset_attribute.attribute.variable_name
            ] = measurement.value
        self.results.append(
            DatasetTimelineValue(
                iter_dt, dt_loc_val, iter_loc, iter_alt
            )
        )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return ObservationReaderValue(
            self.results, self.location_input, self.attributes,
            self.start_date, self.end_date
        )
