# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading dataset
"""

import json
from typing import Union, List, Dict
import numpy as np
from datetime import datetime
import pytz
from django.contrib.gis.geos import Point, Polygon, MultiPolygon, GeometryCollection

from gap.models import (
    Dataset,
    DatasetAttribute
)


class DatasetTimelineValue:
    """Class representing data value for given datetime."""

    def __init__(
            self, datetime: Union[np.datetime64, datetime],
            values: dict) -> None:
        """Initialize DatasetTimelineValue object.

        :param datetime: datetime of data
        :type datetime: np.datetime64 or datetime
        :param values: Dictionary of variable and its value
        :type values: dict
        """
        self.datetime = datetime
        self.values = values

    def _datetime_as_str(self):
        """Convert datetime object to string."""
        if self.datetime is None:
            return ''
        if isinstance(self.datetime, np.datetime64):
            return np.datetime_as_string(
                self.datetime, unit='s', timezone='UTC')
        return self.datetime.isoformat(timespec='seconds')

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of datetime and values
        :rtype: dict
        """
        return {
            'datetime': self._datetime_as_str(),
            'values': self.values
        }


class DatasetReaderValue:
    """Class representing all values from reader."""

    def __init__(
            self, location: Point, results: List[DatasetTimelineValue]) -> None:
        """Initialize DatasetReaderValue object.

        :param location: point to the observed station/grid cell
        :type location: Point
        :param results: Data value list
        :type results: List[DatasetTimelineValue]
        """
        self.location = location
        self.results = results

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        if self.location is None:
            return {}
        return {
            'geometry': json.loads(self.location.json),
            'data': [result.to_dict() for result in self.results]
        }


class LocationDatasetReaderValue(DatasetReaderValue):
    """Class representing data values for multiple locations."""

    def __init__(
            self, results: Dict[Point, List[DatasetTimelineValue]]) -> None:
        """Initialize LocationDatasetReaderValue."""
        super().__init__(None, [])
        self.results = results

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        location_data = []
        for location, values in self.results.items():
            val = DatasetReaderValue(location, values)
            location_data.append(val.to_dict())
        return location_data


class LocationInputType:
    """Class for data input type."""

    POINT = 'point'
    BBOX = 'bbox'
    POLYGON = 'polygon'
    LIST_OF_POINT = 'list_of_point'


class DatasetReaderInput:
    """Class to store the dataset reader input.

    Input type: Point, bbox, polygon, list of point
    TODO: list of points
    """

    def __init__(self, geom_collection: GeometryCollection, type: str):
        """Initialize DatasetReaderInput class."""
        self.geom_collection = geom_collection
        self.type = type

    @property
    def point(self) -> Point:
        if self.type != LocationInputType.POINT:
            raise TypeError('Location input type is not bbox/point!')
        return Point(x=self.geom_collection[0].x, y=self.geom_collection[0].y, srid=4326)

    @property
    def polygon(self) -> MultiPolygon:
        if self.type != LocationInputType.POLYGON:
            raise TypeError('Location input type is not polygon!')
        return self.geom_collection

    @property
    def points(self) -> List[Point]:
        if self.type not in [LocationInputType.BBOX, LocationInputType.LIST_OF_POINT]:
            raise TypeError('Location input type is not bbox/point!')
        return [Point(x=point.x, y=point.y, srid=4326) for point in self.geom_collection]


class BaseDatasetReader:
    """Base class for Dataset Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime) -> None:
        """Initialize BaseDatasetReader class.

        :param dataset: Dataset for reading
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
        self.dataset = dataset
        self.attributes = attributes
        self.location_input = location_input
        self.start_date = start_date
        self.end_date = end_date

    def add_attribute(self, attribute: DatasetAttribute):
        """Add a new attribuute to be read.

        :param attribute: Dataset Attribute
        :type attribute: DatasetAttribute
        """
        is_existing = [a for a in self.attributes if a.id == attribute.id]
        if len(is_existing) == 0:
            self.attributes.append(attribute)

    def get_attributes_metadata(self) -> dict:
        """Get attributes metadata (unit and desc).

        :return: Dictionary of attribute and its metadata
        :rtype: dict
        """
        results = {}
        for attrib in self.attributes:
            results[attrib.attribute.variable_name] = {
                'units': attrib.attribute.unit.name,
                'longname': attrib.attribute.name
            }
        return results

    def read(self):
        """Read values from dataset."""
        today = datetime.now(tz=pytz.UTC)
        if self.start_date < today:
            self.read_historical_data(
                self.start_date,
                self.end_date if self.end_date < today else today
            )
            if self.end_date > today:
                self.read_forecast_data(
                    today, self.end_date
                )
        else:
            self.read_forecast_data(
                self.start_date, self.end_date
            )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        pass


    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        pass

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        pass