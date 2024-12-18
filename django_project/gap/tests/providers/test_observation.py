# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Reader.
"""

from django.test import TestCase
from datetime import datetime
from django.contrib.gis.geos import (
    Point, MultiPoint, MultiPolygon, Polygon
)

from gap.providers import (
    ObservationDatasetReader
)
from gap.providers.observation import ObservationReaderValue
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    StationFactory,
    MeasurementFactory,
    StationTypeFactory
)
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType
)


class TestObservationReader(TestCase):
    """Unit test for ObservationDatasetReader class."""

    def setUp(self):
        """Set test for ObservationDatasetReader."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name='Tahmo'))
        self.attribute = AttributeFactory.create(
            name='Surface air temperature',
            variable_name='surface_air_temperature')
        self.dataset_attr = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute,
            source='surface_air_temperature'
        )
        p = Point(x=26.97, y=-12.56, srid=4326)
        self.station = StationFactory.create(
            geometry=p,
            provider=self.dataset.provider
        )
        self.location_input = DatasetReaderInput.from_point(p)
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], self.location_input,
            self.start_date, self.end_date
        )

    def test_find_nearest_station_empty(self):
        """Test find nearest station empty."""
        new_provider = ProviderFactory(name='test_provider')
        self.station.provider = new_provider
        self.station.save()

        self.reader.read_historical_data(self.start_date, self.end_date)
        data_value = self.reader.get_data_values()
        self.assertTrue(data_value.is_empty())

    def test_find_nearest_station_by_point(self):
        """Test find nearest station from single point."""
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=self.start_date,
            value=120
        )
        result = self.reader._find_nearest_station_by_point()
        self.assertEqual(result, [self.station])

    def test_find_nearest_station_by_bbox(self):
        """Test find nearest station from bbox."""
        self.reader.location_input = DatasetReaderInput.from_bbox(
            [-180, -90, 180, 90])
        result = self.reader._find_nearest_station_by_bbox()
        self.assertEqual(list(result), [self.station])

    def test_find_nearest_station_by_polygon(self):
        """Test find nearest station from polygon."""
        self.reader.location_input.type = LocationInputType.POLYGON
        self.reader.location_input.geom_collection = MultiPolygon(
            Polygon.from_bbox((-180, -90, 180, 90)))
        result = self.reader._find_nearest_station_by_polygon()
        self.assertEqual(list(result), [self.station])

    def test_find_nearest_station_by_points(self):
        """Test find nearest station from list of point."""
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=self.start_date,
            value=120
        )
        self.reader.location_input.type = LocationInputType.LIST_OF_POINT
        self.reader.location_input.geom_collection = MultiPoint(
            [Point(0, 0), self.station.geometry])
        result = self.reader._find_nearest_station_by_points()
        self.assertEqual(list(result), [self.station])

    def test_read_historical_data(self):
        """Test for reading historical data from Tahmo."""
        dt = datetime(2019, 11, 1, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt,
            value=100
        )
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], DatasetReaderInput.from_point(
                self.station.geometry
            ), dt, dt)
        reader.read_historical_data(dt, dt)
        data_value = reader.get_data_values()
        results = data_value.to_json()
        self.assertEqual(len(results['data']), 1)
        self.assertEqual(
            results['data'][0]['values']['surface_air_temperature'],
            100)

    def test_read_historical_data_empty(self):
        """Test for reading empty historical data from Tahmo."""
        dt = datetime(2019, 11, 1, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt,
            value=100
        )
        points = DatasetReaderInput(MultiPoint(
            [Point(0, 0), Point(1, 1)]), LocationInputType.LIST_OF_POINT)
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], points, dt, dt)
        reader.read_historical_data(
            datetime(2010, 11, 1, 0, 0, 0),
            datetime(2010, 11, 1, 0, 0, 0))
        data_value = reader.get_data_values()
        self.assertEqual(data_value.count(), 0)

    def test_read_historical_data_by_point(self):
        """Test read by stations that has same point for different dataset."""
        other_dataset = DatasetFactory.create(
            provider=self.dataset.provider)
        other_attr = DatasetAttributeFactory.create(
            dataset=other_dataset,
            attribute=self.attribute,
            source='test_attr'
        )
        other_station = StationFactory.create(
            geometry=self.station.geometry,
            provider=self.dataset.provider,
            station_type=StationTypeFactory.create(name='other')
        )

        # create measurement
        dt1 = datetime(2019, 11, 1, 0, 0, 0)
        dt2 = datetime(2019, 11, 2, 0, 0, 0)
        measurement = MeasurementFactory.create(
            station=other_station,
            dataset_attribute=other_attr,
            date_time=dt1,
            value=876
        )

        # create reader
        location_input = DatasetReaderInput.from_point(other_station.geometry)
        reader = ObservationDatasetReader(
            other_dataset, [other_attr], location_input,
            dt1, dt2
        )
        reader.read_historical_data(dt1, dt2)
        data_value = reader.get_data_values()
        # should return above measurement
        self.assertEqual(len(data_value._val), 1)
        dict_value = data_value.to_json()
        self.assertEqual(
            dict_value['data'][0]['values']['surface_air_temperature'],
            measurement.value
        )

    def test_read_historical_data_multiple_locations(self):
        """Test for reading historical data from multiple locations."""
        dt1 = datetime(2019, 11, 1, 0, 0, 0)
        dt2 = datetime(2019, 11, 2, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt1,
            value=100
        )
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt2,
            value=200
        )
        p = Point(x=28.97, y=-10.56, srid=4326)
        station2 = StationFactory.create(
            geometry=p,
            provider=self.dataset.provider
        )
        MeasurementFactory.create(
            station=station2,
            dataset_attribute=self.dataset_attr,
            date_time=dt1,
            value=300
        )
        location_input = DatasetReaderInput(
            MultiPoint([self.station.geometry, p]),
            LocationInputType.LIST_OF_POINT)
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], location_input, dt1, dt2)
        reader.read_historical_data(dt1, dt2)
        data_value = reader.get_data_values()
        self.assertEqual(len(data_value._val), 3)

    def test_observation_to_netcdf_stream(self):
        """Test convert observation value to netcdf stream."""
        dt1 = datetime(2019, 11, 1, 0, 0, 0)
        dt2 = datetime(2019, 11, 2, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt1,
            value=100
        )
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt2,
            value=200
        )
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], self.location_input,
            dt1, dt2
        )
        qs = reader.get_measurements(dt1, dt2)
        reader_value = ObservationReaderValue(
            qs, self.location_input, [self.dataset_attr],
            self.start_date, self.end_date, [self.station],
            qs.count()
        )
        d = reader_value.to_netcdf_stream()
        res = list(d)
        self.assertIsNotNone(res)
