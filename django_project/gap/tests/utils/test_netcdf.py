# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF Utilities.
"""

import os
import json
from django.test import TestCase
from datetime import datetime
import numpy as np
import xarray as xr
import pandas as pd
from django.contrib.gis.geos import (
    Point, GeometryCollection, Polygon, MultiPolygon,
    MultiPoint
)
from unittest.mock import Mock, MagicMock, patch

from gap.models import Provider, Dataset, DatasetAttribute
from gap.utils.reader import (
    DatasetTimelineValue,
    DatasetReaderValue,
    DatasetReaderInput,
    LocationInputType
)
from gap.utils.netcdf import (
    NetCDFProvider,
    BaseNetCDFReader,
    daterange_inc,
)
from gap.providers import (
    CBAMNetCDFReader,
    SalientZarrReader,
    CBAMZarrReader,
    get_reader_from_dataset,
    ObservationParquetReader,
    ObservationAirborneParquetReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory
)


class TestNetCDFProvider(TestCase):
    """Unit test for NetCDFProvider class."""

    def setUp(self):
        """Set test for NetCDFProvider class."""
        self.provider = Provider(name='CBAM')
        self.env_vars = {
            'CBAM_AWS_ACCESS_KEY_ID': 'test_key',
            'CBAM_AWS_SECRET_ACCESS_KEY': 'test_secret',
            'CBAM_AWS_ENDPOINT_URL': 'https://test.endpoint',
            'CBAM_AWS_BUCKET_NAME': 'test_bucket',
            'CBAM_AWS_DIR_PREFIX': 'test_prefix',
            'CBAM_AWS_REGION_NAME': 'test_region'
        }

    def test_constants(self):
        """Test for correct constants."""
        self.assertEqual(NetCDFProvider.CBAM, 'CBAM')
        self.assertEqual(NetCDFProvider.SALIENT, 'Salient')

    @patch.dict(os.environ, {
        'CBAM_AWS_ACCESS_KEY_ID': 'test_key',
        'CBAM_AWS_SECRET_ACCESS_KEY': 'test_secret',
        'CBAM_AWS_ENDPOINT_URL': 'https://test.endpoint',
        'CBAM_AWS_BUCKET_NAME': 'test_bucket',
        'CBAM_AWS_DIR_PREFIX': 'test_prefix',
        'CBAM_AWS_REGION_NAME': 'test_region'
    })
    def test_get_s3_variables(self):
        """Test get_s3_variables method."""
        expected = {
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret',
            'AWS_ENDPOINT_URL': 'https://test.endpoint',
            'AWS_BUCKET_NAME': 'test_bucket',
            'AWS_DIR_PREFIX': 'test_prefix',
            'AWS_REGION_NAME': 'test_region'
        }
        self.assertEqual(
            NetCDFProvider.get_s3_variables(self.provider), expected)

    @patch.dict(os.environ, {
        'CBAM_AWS_ENDPOINT_URL': 'https://test.endpoint',
        'CBAM_AWS_REGION_NAME': 'test_region'
    })
    def test_get_s3_client_kwargs(self):
        """Test for get_s3_client_kwargs."""
        expected = {
            'endpoint_url': 'https://test.endpoint',
            'region_name': 'test_region'
        }
        self.assertEqual(
            NetCDFProvider.get_s3_client_kwargs(self.provider), expected)


class TestDaterangeInc(TestCase):
    """Unit test for daterange_inc function."""

    def test_daterange(self):
        """Test for daterance_inc function."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 3)
        expected_dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3)
        ]
        result_dates = list(daterange_inc(start_date, end_date))
        self.assertEqual(result_dates, expected_dates)


class TestDatasetTimelineValue(TestCase):
    """Unit test for class DatasetTimelineValue."""

    def setUp(self):
        """Set test for TestDatasetTimelineValue class."""
        self.point = Point(0, 0)

    def test_to_dict_with_datetime(self):
        """Test to_dict with python datetime object."""
        dt = datetime(2023, 7, 16, 12, 0, 0)
        values = {"temperature": 25}
        dtv = DatasetTimelineValue(dt, values, self.point)
        expected = {
            'datetime': '2023-07-16T12:00:00',
            'values': values
        }
        self.assertEqual(dtv.to_dict(), expected)

    def test_to_dict_with_np_datetime64(self):
        """Test to_dict with numpy datetime64 object."""
        dt = np.datetime64('2023-07-16T12:00:00')
        values = {"temperature": 25}
        dtv = DatasetTimelineValue(dt, values, self.point)
        expected = {
            'datetime': np.datetime_as_string(dt, unit='s', timezone='UTC'),
            'values': values
        }
        self.assertEqual(dtv.to_dict(), expected)

    def test_to_dict_with_none_datetime(self):
        """Test to_dict with empty datetime."""
        dtv = DatasetTimelineValue(None, {"temperature": 25}, self.point)
        expected = {
            'datetime': '',
            'values': {"temperature": 25}
        }
        self.assertEqual(dtv.to_dict(), expected)


class TestDatasetReaderValue(TestCase):
    """Unit test for class DatasetReaderValue."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set test for TestDatasetReaderValue class."""
        self.point = Point(0, 0)
        self.dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        self.attribute = DatasetAttribute.objects.filter(
            dataset=self.dataset,
            attribute__variable_name='max_temperature'
        ).first()

        # Creating mock xarray dataset
        dates = pd.date_range("2021-01-01", periods=10)
        lats = np.array([10, 20])
        lons = np.array([30, 40])
        temperature_data = np.random.rand(len(dates), len(lats), len(lons))
        self.mock_xr_dataset = xr.Dataset(
            {
                "max_total_temperature": (
                    ["date", "lat", "lon"], temperature_data),
            },
            coords={
                "date": dates,
                "lat": lats,
                "lon": lons,
            }
        )

        point2 = Point(1, 1, srid=4326)
        self.mock_location_input = DatasetReaderInput.from_point(point2)
        # DatasetReaderValue initialization with xarray dataset
        self.dataset_reader_value_xr = DatasetReaderValue(
            val=self.mock_xr_dataset,
            location_input=self.mock_location_input,
            attributes=[self.attribute]
        )

        # DatasetReaderValue initialization with list
        self.dataset_reader_value_list = DatasetReaderValue(
            val=[],
            location_input=self.mock_location_input,
            attributes=[self.attribute]
        )

    def test_to_dict_with_location(self):
        """Test to_dict with location."""
        location = Point(1, 1)
        location_input = DatasetReaderInput.from_point(location)
        dtv = DatasetTimelineValue(
            datetime(2023, 7, 16, 12, 0, 0), {"temperature": 25}, self.point)
        drv = DatasetReaderValue([dtv], location_input, [])
        expected = {
            'geometry': json.loads(location.json),
            'data': [dtv.to_dict()]
        }
        self.assertEqual(drv._to_dict(), expected)

    def test_to_dict_with_none_location(self):
        """Test to_dict with empty location."""
        drv = DatasetReaderValue([], None, [])
        expected = {}
        self.assertEqual(drv._to_dict(), expected)

    def test_initialization_with_xr_dataset(self):
        """Test init with xarray dataset."""
        self.assertTrue(self.dataset_reader_value_xr._is_xr_dataset)

    def test_initialization_with_list(self):
        """Test init with list."""
        self.assertFalse(self.dataset_reader_value_list._is_xr_dataset)
        self.assertEqual(self.dataset_reader_value_list.values, [])

    def test_is_empty(self):
        """Test check value is empty."""
        self.assertFalse(self.dataset_reader_value_xr.is_empty())
        self.assertTrue(self.dataset_reader_value_list.is_empty())

    def test_to_json_with_point_type(self):
        """Test empty convert to json."""
        result = self.dataset_reader_value_list.to_json()
        self.assertEqual(result, {})

    def test_to_json_with_non_point_polygon_type(self):
        """Test invalid convert to json."""
        self.mock_location_input.type = 'polygon'
        self.dataset_reader_value_list.to_json()

        self.mock_location_input.type = 'bbox'
        with self.assertRaises(TypeError):
            self.dataset_reader_value_list.to_json()

    def test_to_csv_stream(self):
        """Test convert to csv."""
        csv_stream = self.dataset_reader_value_xr.to_csv_stream()
        csv_data = list(csv_stream)
        self.assertIsNotNone(csv_data)
        data = []
        for idx, d in enumerate(csv_data):
            if idx == 0:
                continue
            data.extend(d.splitlines())

        # rows without header
        self.assertEqual(len(data), 40)

    def test_to_netcdf_stream(self):
        """Test convert to netcdf."""
        d = self.dataset_reader_value_xr.to_netcdf_stream()
        res = list(d)
        self.assertIsNotNone(res)


class TestDatasetReaderInput(TestCase):
    """Unit test for DatasetReaderInput class."""

    def test_point(self):
        """Test get property point."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.POINT)
        self.assertEqual(dri.point, Point(1, 1, srid=4326))

    def test_polygon(self):
        """Test get property polygon."""
        polygon = MultiPolygon(Polygon(((0, 0), (1, 0), (1, 1), (0, 0))))
        geom_collection = GeometryCollection(polygon)
        dri = DatasetReaderInput(geom_collection, LocationInputType.POLYGON)
        self.assertEqual(dri.polygon, geom_collection)

    def test_points(self):
        """Test get property points."""
        points = [Point(1, 1), Point(2, 2)]
        geom_collection = GeometryCollection(*points)
        dri = DatasetReaderInput(
            geom_collection, LocationInputType.LIST_OF_POINT)
        self.assertEqual(
            dri.points, [Point(1, 1, srid=4326), Point(2, 2, srid=4326)])

    def test_from_point(self):
        """Test create object from point."""
        point = Point(1, 1, srid=4326)
        dri = DatasetReaderInput.from_point(point)
        self.assertEqual(dri.type, LocationInputType.POINT)
        self.assertEqual(dri.point, point)

    def test_from_bbox(self):
        """Test create object from bbox."""
        bbox_list = [1.0, 1.0, 2.0, 2.0]
        dri = DatasetReaderInput.from_bbox(bbox_list)
        self.assertEqual(dri.type, LocationInputType.BBOX)
        expected_points = [
            Point(x=bbox_list[0], y=bbox_list[1], srid=4326),
            Point(x=bbox_list[2], y=bbox_list[3], srid=4326)
        ]
        self.assertEqual(dri.points, expected_points)

    def test_invalid_point_access(self):
        """Test get property point with invalid type."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.BBOX)
        with self.assertRaises(TypeError):
            _ = dri.point

    def test_invalid_polygon_access(self):
        """Test get property polygon with invalid type."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.POINT)
        with self.assertRaises(TypeError):
            _ = dri.polygon

    def test_invalid_points_access(self):
        """Test get property points with invalid type."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.POINT)
        with self.assertRaises(TypeError):
            _ = dri.points


class TestBaseNetCDFReader(TestCase):
    """Unit test for class BaseNetCDFReader."""

    def test_read_variables_base_functions(self):
        """Test base functions for reading variables."""
        attrib = DatasetAttributeFactory.create()
        attrib.attribute.variable_name = 'temperature'
        attrib.attribute.unit.name = 'C'
        attrib.attribute.name = 'Temperature'
        reader = BaseNetCDFReader(Mock(), [attrib], Mock(), Mock(), Mock())
        self.assertFalse(
            reader._read_variables_by_point(
                attrib.dataset, ['temperature'], Mock(), Mock())
        )
        self.assertFalse(
            reader._read_variables_by_bbox(
                attrib.dataset, ['temperature'], Mock(), Mock())
        )
        self.assertFalse(
            reader._read_variables_by_polygon(
                attrib.dataset, ['temperature'], Mock(), Mock())
        )
        self.assertFalse(
            reader._read_variables_by_points(
                attrib.dataset, ['temperature'], Mock(), Mock())
        )


class TestCBAMNetCDFReader(TestCase):
    """Unit test for class CBAMNetCDFReader."""

    def setUp(self):
        """Set test for CBAMNetCDFReader."""
        self.provider = Provider(name='CBAM')
        self.dataset = Dataset(provider=self.provider)
        self.attributes = [DatasetAttribute(source='var1'),
                           DatasetAttribute(source='var2')]
        self.location_input = DatasetReaderInput.from_point(
            Point(1.0, 2.0)
        )
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = CBAMNetCDFReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    def test_add_attribute(self):
        """Test adding a new attribute to Reader."""
        reader = CBAMNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        self.assertEqual(len(reader.attributes), 0)
        reader.add_attribute(DatasetAttributeFactory.create())
        self.assertEqual(len(reader.attributes), 1)

    def test_get_attributes_metadata(self):
        """Test get attributes metadata dict."""
        attrib = DatasetAttributeFactory.create()
        attrib.attribute.variable_name = 'temperature'
        attrib.attribute.unit.name = 'C'
        attrib.attribute.name = 'Temperature'
        reader = CBAMNetCDFReader(Mock(), [attrib], Mock(), Mock(), Mock())
        expected = {
            'temperature': {
                'units': 'C',
                'longname': 'Temperature'
            }
        }
        self.assertEqual(reader.get_attributes_metadata(), expected)

    def test_read_variables(self):
        """Test reading variables."""
        dataset = DatasetFactory.create(name=NetCDFProvider.CBAM)
        attribute = AttributeFactory.create(
            name='Temperature', variable_name='temperature')
        dataset_attr = DatasetAttributeFactory(
            dataset=dataset, attribute=attribute)
        reader = CBAMNetCDFReader(
            dataset, [dataset_attr],
            DatasetReaderInput.from_point(Point(x=29.125, y=-2.215)),
            Mock(), Mock())
        xrArray = MagicMock()
        xrArray.sel.return_value = []
        xrDataset = MagicMock()
        xrDataset.__getitem__.return_value = xrArray
        result = reader.read_variables(xrDataset)
        self.assertEqual(result, [])

    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_client_kwargs')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_variables')
    @patch('fsspec.filesystem')
    def test_setup_reader(
        self, mock_filesystem, mock_get_s3_vars, mock_get_s3_kwargs):
        """Test for setup NetCDFReader class."""
        mock_get_s3_kwargs.return_value = {
            'endpoint_url': 'test_endpoint'
        }
        mock_get_s3_vars.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_key_id',
            'AWS_SECRET_ACCESS_KEY': 'test_key_secret',
        }
        reader = CBAMNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.setup_reader()
        mock_filesystem.assert_called_once_with(
            's3',
            key='test_key_id',
            secret='test_key_secret',
            client_kwargs=dict(endpoint_url='test_endpoint'))

    @patch('xarray.open_dataset')
    def test_open_dataset(self, mock_open_dataset):
        """Test for opening a dataset."""
        reader = CBAMNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.fs = Mock()
        reader.s3 = {
            'AWS_DIR_PREFIX': '',
            'AWS_BUCKET_NAME': 'test_bucket'
        }
        netcdf_file = Mock()
        netcdf_file.name = 'test_file.nc'
        reader.open_dataset(netcdf_file)
        mock_open_dataset.assert_called_once()

    def test_from_dataset(self):
        """Test for creating NetCDFReader from dataset."""
        dataset1 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        reader = get_reader_from_dataset(dataset1)
        self.assertEqual(reader, CBAMZarrReader)
        dataset2 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        reader = get_reader_from_dataset(dataset2)
        self.assertEqual(reader, SalientZarrReader)
        # invalid type
        dataset3 = DatasetFactory.create()
        with self.assertRaises(TypeError):
            get_reader_from_dataset(dataset3)
        # use_parquet True
        dataset3 = DatasetFactory.create(
            provider=ProviderFactory(name='Arable'))
        reader = get_reader_from_dataset(dataset3, use_parquet=True)
        self.assertEqual(reader, ObservationParquetReader)
        dataset4 = DatasetFactory.create(
            provider=ProviderFactory(name='WindBorne Systems'))
        reader = get_reader_from_dataset(dataset4, use_parquet=True)
        self.assertEqual(reader, ObservationAirborneParquetReader)

    def test_read_variables_by_point(self):
        """Test read variables xarray by point."""
        # Prepare a mock dataset
        data = np.random.rand(2, 10, 10)
        lats = np.linspace(-90, 90, 10)
        lons = np.linspace(-180, 180, 10)
        times = np.array(['2020-01-01', '2020-01-02'], dtype='datetime64')
        dataset = xr.Dataset(
            {'var1': (['time', 'lat', 'lon'], data),
             'var2': (['time', 'lat', 'lon'], data)},
            coords={'time': times, 'lat': lats, 'lon': lons}
        )
        result = self.reader._read_variables_by_point(
            dataset, ['var1', 'var2'],
            np.datetime64(self.start_date), np.datetime64(self.end_date)
        )
        self.assertIsInstance(result, xr.Dataset)

    def test_read_variables_by_bbox(self):
        """Test read variables xarray by bbox."""
        data = np.random.rand(2, 10, 10)
        lats = np.linspace(-90, 90, 10)
        lons = np.linspace(-180, 180, 10)
        times = np.array(['2020-01-01', '2020-01-02'], dtype='datetime64')
        dataset = xr.Dataset(
            {'var1': (['time', 'lat', 'lon'], data),
             'var2': (['time', 'lat', 'lon'], data)},
            coords={'time': times, 'lat': lats, 'lon': lons}
        )
        self.reader.location_input = DatasetReaderInput.from_bbox(
            [-180, -90, 180, 90])
        result = self.reader._read_variables_by_bbox(
            dataset, ['var1', 'var2'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )
        self.assertIsInstance(result, xr.Dataset)

    def test_read_variables_by_points(self):
        """Test read variables xarray by points."""
        data = np.random.rand(2, 10, 10)
        lats = np.linspace(-90, 90, 10)
        lons = np.linspace(-180, 180, 10)
        times = np.array(['2020-01-01', '2020-01-02'], dtype='datetime64')
        dataset = xr.Dataset(
            {'var1': (['time', 'lat', 'lon'], data),
             'var2': (['time', 'lat', 'lon'], data)},
            coords={'time': times, 'lat': lats, 'lon': lons}
        )
        self.reader.location_input.type = LocationInputType.LIST_OF_POINT
        self.reader.location_input.geom_collection = MultiPoint(
            [Point(0, 0), Point(10, 10)])
        result = self.reader._read_variables_by_points(
            dataset, ['var1', 'var2'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )
        self.assertIsInstance(result, xr.Dataset)

    def test_find_locations(self):
        """Test find locations method."""
        data = np.random.rand(10, 10)
        lats = np.linspace(-90, 90, 10)
        lons = np.linspace(-180, 180, 10)
        dataset = xr.Dataset(
            {'var1': (['lat', 'lon'], data)},
            coords={'lat': lats, 'lon': lons}
        )
        locations, lat_len, lon_len = self.reader.find_locations(dataset)
        self.assertEqual(len(locations), 100)
        self.assertEqual(lat_len, 10)
        self.assertEqual(lon_len, 10)

    @patch.object(CBAMNetCDFReader, '_read_variables_by_point')
    @patch.object(CBAMNetCDFReader, '_read_variables_by_bbox')
    @patch.object(CBAMNetCDFReader, '_read_variables_by_polygon')
    @patch.object(CBAMNetCDFReader, '_read_variables_by_points')
    def test_read_variables_several_cases(
        self, mock_read_by_points, mock_read_by_polygon,
        mock_read_by_bbox, mock_read_by_point):
        """Test several cases in read_variables function."""
        data = np.random.rand(2, 10, 10)
        lats = np.linspace(-90, 90, 10)
        lons = np.linspace(-180, 180, 10)
        times = np.array(['2020-01-01', '2020-01-02'], dtype='datetime64')
        dataset = xr.Dataset(
            {'var1': (['time', 'lat', 'lon'], data),
             'var2': (['time', 'lat', 'lon'], data)},
            coords={'time': times, 'lat': lats, 'lon': lons}
        )

        # Mock results
        mock_read_by_point.return_value = dataset
        mock_read_by_bbox.return_value = dataset
        mock_read_by_polygon.return_value = dataset
        mock_read_by_points.return_value = dataset

        # Test POINT input
        self.reader.location_input.type = LocationInputType.POINT
        result = self.reader.read_variables(
            dataset, self.start_date, self.end_date)
        self.assertIsInstance(result, xr.Dataset)
        mock_read_by_point.assert_called_once_with(
            dataset, ['var1', 'var2', 'date'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )

        # Test BBOX input
        self.reader.location_input = DatasetReaderInput.from_bbox(
            [-180, -90, 180, 90])
        result = self.reader.read_variables(
            dataset, self.start_date, self.end_date)
        self.assertIsInstance(result, xr.Dataset)
        mock_read_by_bbox.assert_called_once_with(
            dataset, ['var1', 'var2', 'date'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )

        # Test POLYGON input
        self.reader.location_input.type = LocationInputType.POLYGON
        polygon = Polygon(((0, 0), (0, 10), (10, 10), (10, 0), (0, 0)))
        self.reader.location_input.geom_collection = MultiPolygon(polygon)
        result = self.reader.read_variables(
            dataset, self.start_date, self.end_date)
        self.assertIsInstance(result, xr.Dataset)
        mock_read_by_polygon.assert_called_once_with(
            dataset, ['var1', 'var2', 'date'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )

        # Test LIST_OF_POINT input
        self.reader.location_input.type = LocationInputType.LIST_OF_POINT
        points = [Point(0, 0), Point(10, 10)]
        self.reader.location_input.geom_collection = GeometryCollection(
            *points)
        result = self.reader.read_variables(
            dataset, self.start_date, self.end_date)
        self.assertIsInstance(result, xr.Dataset)
        mock_read_by_points.assert_called_once_with(
            dataset, ['var1', 'var2', 'date'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )

        # Test exception handling
        mock_read_by_bbox.reset_mock()
        mock_read_by_bbox.side_effect = Exception('Unexpected error')
        self.reader.location_input = DatasetReaderInput.from_bbox(
            [-180, -90, 180, 90])
        result = self.reader.read_variables(
            dataset, self.start_date, self.end_date)
        self.assertIsNone(result)
        mock_read_by_bbox.assert_called_once_with(
            dataset, ['var1', 'var2', 'date'], np.datetime64(self.start_date),
            np.datetime64(self.end_date)
        )
