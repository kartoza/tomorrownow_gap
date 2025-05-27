# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Reader.
"""

from django.test import TestCase
from datetime import datetime
import xarray as xr
import numpy as np
import pandas as pd
from django.contrib.gis.geos import Point, MultiPoint
from unittest.mock import Mock, patch

from core.settings.utils import absolute_path
from gap.models import DatasetAttribute, Dataset, DatasetStore
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType,
    DatasetReaderValue
)
from gap.utils.netcdf import (
    NetCDFProvider,
)
from gap.providers.salient import (
    SalientNetCDFReader,
    SalientReaderValue,
    SalientZarrReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    DataSourceFileFactory
)


class TestSalientReaderValue(TestCase):
    """Unit test for class SalientReaderValue."""

    fixtures = [
        '1.object_storage_manager.json',
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set TestSalientReaderValue class."""
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        # Mocking DatasetAttribute
        self.attribute = DatasetAttribute.objects.filter(
            dataset=self.dataset,
            attribute__variable_name='temperature'
        ).first()

        # Creating mock DatasetReaderInput
        point = Point(30, 10, srid=4326)
        self.mock_location_input = DatasetReaderInput.from_point(point)

        # Creating filtered xarray dataset
        forecast_days = np.array([0, 1, 2])
        lats = np.array([10, 20])
        lons = np.array([30, 40])
        temperature_data = np.random.rand(
            len(forecast_days), len(lats), len(lons))

        self.mock_xr_dataset = xr.Dataset(
            {
                "temp": (
                    ["forecast_day_idx", "lat", "lon"], temperature_data
                ),
            },
            coords={
                "forecast_day_idx": forecast_days,
                "lat": lats,
                "lon": lons,
            }
        )
        # Mock forecast_date
        self.forecast_date = np.datetime64('2023-01-01')
        variables = [
            'forecast_day_idx',
            'temp'
        ]
        self.mock_xr_dataset = self.mock_xr_dataset[variables].sel(
            lat=point.y,
            lon=point.x, method='nearest'
        ).where(
            (self.mock_xr_dataset['forecast_day_idx'] >= 0) &
            (self.mock_xr_dataset['forecast_day_idx'] <= 1),
            drop=True)

        # SalientReaderValue initialization with xarray dataset
        self.salient_reader_value_xr = SalientReaderValue(
            val=self.mock_xr_dataset,
            location_input=self.mock_location_input,
            attributes=[self.attribute],
            forecast_date=self.forecast_date,
            is_from_zarr=True
        )

    def test_initialization(self):
        """Test initialization method."""
        self.assertEqual(
            self.salient_reader_value_xr.forecast_date, self.forecast_date)
        self.assertTrue(self.salient_reader_value_xr._is_from_zarr)
        self.assertTrue(self.salient_reader_value_xr._is_xr_dataset)

    def test_post_init(self):
        """Test post initialization method."""
        # Check if the renaming happened correctly
        self.assertIn(
            'forecast_day', self.salient_reader_value_xr.xr_dataset.coords)
        self.assertIn(
            'temperature', self.salient_reader_value_xr.xr_dataset.data_vars)
        self.assertNotIn(
            'forecast_day_idx', self.salient_reader_value_xr.xr_dataset.coords
        )

        # Check if forecast_day has been updated to actual dates
        forecast_days = pd.date_range('2023-01-01', periods=2)
        xr_forecast_days = pd.to_datetime(
            self.salient_reader_value_xr.xr_dataset.forecast_day.values)
        pd.testing.assert_index_equal(
            pd.Index(xr_forecast_days), forecast_days)

    def test_is_empty(self):
        """Test is_empty method."""
        self.assertFalse(self.salient_reader_value_xr.is_empty())

    def test_to_json_with_point_type(self):
        """Test convert to_json with point."""
        result = self.salient_reader_value_xr.to_json()
        self.assertIn('geometry', result)
        self.assertIn('data', result)
        self.assertIsInstance(result['data'], list)

    def test_to_json_with_non_point_type(self):
        """Test convert to_json with exception."""
        self.mock_location_input.type = 'polygon'
        with self.assertRaises(TypeError):
            self.salient_reader_value_xr.to_json()

    def test_xr_dataset_to_dict(self):
        """Test convert xarray dataset to dict."""
        result_dict = self.salient_reader_value_xr._xr_dataset_to_dict()
        self.assertIn('geometry', result_dict)
        self.assertIn('data', result_dict)
        self.assertIsInstance(result_dict['data'], list)


class TestSalientNetCDFReader(TestCase):
    """Unit test for Salient NetCDFReader class."""

    fixtures = [
        '1.object_storage_manager.json',
    ]

    def setUp(self):
        """Set Test class for SalientNetCDFReader."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        self.attribute1 = AttributeFactory.create(
            name='Temperature Climatology',
            variable_name='temp_clim')
        self.dataset_attr1 = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute1,
            source='temp_clim'
        )
        self.attribute2 = AttributeFactory.create(
            name='Precipitation Anomaly',
            variable_name='precip_anom')
        self.dataset_attr2 = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute2,
            source='precip_anom'
        )
        self.attributes = [DatasetAttribute(source='var1'),
                           DatasetAttribute(source='var2')]
        self.location_input = DatasetReaderInput.from_point(
            Point(1.0, 2.0)
        )
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = SalientNetCDFReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @patch('gap.models.DataSourceFile.objects.filter')
    @patch('xarray.open_dataset')
    def test_read_forecast_data_empty(self, mock_open_dataset, mock_filter):
        """Test for reading forecast data."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = SalientNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.order_by.return_value.last.return_value = (
            None
        )
        reader.read_forecast_data(start_date, end_date)
        self.assertEqual(reader.xrDatasets, [])

    def test_read_forecast_data(self):
        """Test for reading forecast data from Salient sample."""
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.12, y=-2.625)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            reader = SalientNetCDFReader(
                self.dataset, [self.dataset_attr1, self.dataset_attr2],
                DatasetReaderInput.from_point(p), dt1, dt2)
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values().to_json()
            mock_open.assert_called_once()
            result_data = data_value['data']
            self.assertEqual(len(result_data), 3)
            self.assertAlmostEqual(
                result_data[0]['values']['temp_clim'], 19.461235, 6)
            self.assertEqual(
                len(result_data[0]['values']['precip_anom']), 50)

    def test_read_from_bbox(self):
        """Test for reading forecast data using bbox."""
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.125, y=-2.625)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            reader = SalientNetCDFReader(
                self.dataset, [self.dataset_attr1, self.dataset_attr2],
                DatasetReaderInput.from_bbox(
                    [p.x, p.y, p.x + 0.5, p.y + 0.5]),
                dt1, dt2
            )
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            # temp_clim
            val = dataset['temp_clim'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertAlmostEqual(
                val.values[0], 19.461235, 6)
            # precip_anom
            val = dataset['precip_anom'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertEqual(len(val.values[:, 0]), 50)

    def test_read_from_points(self):
        """Test for reading forecast data using points."""
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.125, y=-2.625)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            location_input = DatasetReaderInput(
                MultiPoint([p, Point(x=p.x + 0.5, y=p.y + 0.5)]),
                LocationInputType.LIST_OF_POINT)
            reader = SalientNetCDFReader(
                self.dataset, [self.dataset_attr1, self.dataset_attr2],
                location_input, dt1, dt2)
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            # temp_clim
            val = dataset['temp_clim'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertAlmostEqual(
                val.values[0], 19.461235, 6)
            # precip_anom
            val = dataset['precip_anom'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertEqual(len(val.values[:, 0]), 50)


class TestSalientZarrReader(TestCase):
    """Unit test for Salient SalientZarrReader class."""

    fixtures = [
        '1.object_storage_manager.json',
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set TestSalientReaderValue class."""
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        self.dataset_attr1 = DatasetAttribute.objects.get(
            dataset=self.dataset,
            source='temp_clim'
        )
        self.dataset_attr2 = DatasetAttribute.objects.get(
            dataset=self.dataset,
            source='precip_anom'
        )
        self.location_input = DatasetReaderInput.from_point(
            Point(1.0, 2.0)
        )

    def test_read_past_forecast_date(self):
        """Test for reading past forecast date."""
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.12, y=-2.625)
        latest = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            is_latest=True
        )
        historical = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            is_latest=False,
            metadata={
                'is_historical': True
            }
        )
        reader = SalientZarrReader(
            self.dataset, [self.dataset_attr1, self.dataset_attr2],
            DatasetReaderInput.from_point(p),
            dt1,
            dt2,
            forecast_date=datetime(2024, 3, 13, 0, 0, 0)
        )
        zarr_file = reader._find_zarr_file()
        self.assertEqual(zarr_file.id, historical.id)

        reader.request_forecast_date = None
        zarr_file = reader._find_zarr_file()
        self.assertEqual(zarr_file.id, latest.id)
