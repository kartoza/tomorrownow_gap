# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Reader.
"""

from django.test import TestCase
from datetime import datetime, timedelta
import pytz
import xarray as xr
import numpy as np
import pandas as pd
from django.contrib.gis.geos import Point, MultiPoint
from unittest.mock import Mock, patch
from io import StringIO

from gap.models import (
    DatasetAttribute, Dataset, DatasetStore,
    Attribute
)
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType,
    DatasetReaderValue
)
from gap.providers.tio import (
    TioZarrReaderValue,
    TioZarrReader
)
from gap.factories import (
    DataSourceFileFactory
)
from gap.tests.ingestor.test_tio_shortterm_ingestor import (
    mock_open_zarr_dataset,
    LAT_METADATA,
    LON_METADATA,
    mock_hourly_open_zarr_dataset
)


class TestTioZarrReaderValue(TestCase):
    """Unit test for class TioZarrReaderValue."""

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
        """Set TestTioZarrReaderValue class."""
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        # Mocking DatasetAttribute
        self.attribute = DatasetAttribute.objects.filter(
            dataset=self.dataset,
            attribute__variable_name='max_temperature'
        ).first()

        # Creating mock DatasetReaderInput
        point = Point(30, 10, srid=4326)
        self.mock_location_input = DatasetReaderInput.from_point(point)

        # Creating filtered xarray dataset
        forecast_days = pd.date_range(start='2023-01-01', end='2023-01-03')
        lats = np.array([10, 20])
        lons = np.array([30, 40])
        temperature_data = np.random.rand(
            len(forecast_days), len(lats), len(lons))

        self.mock_xr_dataset = xr.Dataset(
            {
                "max_temperature": (
                    ["date", "lat", "lon"], temperature_data
                ),
            },
            coords={
                "date": forecast_days,
                "lat": lats,
                "lon": lons,
            }
        )
        # Mock forecast_date
        self.forecast_date = np.datetime64('2023-01-01')
        variables = [
            'date',
            'max_temperature'
        ]
        self.mock_xr_dataset = self.mock_xr_dataset[variables].sel(
            lat=point.y,
            lon=point.x, method='nearest'
        )

        # TioZarrReaderValue initialization with xarray dataset
        self.tio_reader_value_xr = TioZarrReaderValue(
            val=self.mock_xr_dataset,
            location_input=self.mock_location_input,
            attributes=[self.attribute],
            forecast_date=self.forecast_date,
            start_datetime=None,
            end_datetime=None,
        )

    def test_initialization(self):
        """Test initialization method."""
        self.assertEqual(
            self.tio_reader_value_xr.forecast_date, self.forecast_date)
        self.assertTrue(self.tio_reader_value_xr._is_xr_dataset)

    def test_post_init(self):
        """Test post initialization method."""
        # Check if the renaming happened correctly
        self.assertIn(
            'date', self.tio_reader_value_xr.xr_dataset.coords)
        self.assertIn(
            'max_temperature', self.tio_reader_value_xr.xr_dataset.data_vars)
        self.assertNotIn(
            'forecast_day_idx', self.tio_reader_value_xr.xr_dataset.coords
        )
        self.assertNotIn(
            'forecast_date', self.tio_reader_value_xr.xr_dataset.coords
        )

        # Check if forecast_day has been updated to actual dates
        forecast_days = pd.date_range('2023-01-01', periods=3)
        xr_forecast_days = pd.to_datetime(
            self.tio_reader_value_xr.xr_dataset.date.values)
        pd.testing.assert_index_equal(
            pd.Index(xr_forecast_days), forecast_days)

    def test_is_empty(self):
        """Test is_empty method."""
        self.assertFalse(self.tio_reader_value_xr.is_empty())

    def test_to_json_with_point_type(self):
        """Test convert to_json with point."""
        result = self.tio_reader_value_xr.to_json()
        self.assertIn('geometry', result)
        self.assertIn('data', result)
        self.assertIsInstance(result['data'], list)

    def test_to_json_with_non_point_type(self):
        """Test convert to_json with exception."""
        self.mock_location_input.type = 'polygon'
        with self.assertRaises(TypeError):
            self.tio_reader_value_xr.to_json()

    def test_xr_dataset_to_dict(self):
        """Test convert xarray dataset to dict."""
        result_dict = self.tio_reader_value_xr._xr_dataset_to_dict()
        self.assertIn('geometry', result_dict)
        self.assertIn('data', result_dict)
        self.assertIsInstance(result_dict['data'], list)


class TestTioZarrReader(TestCase):
    """Unit test for Tio Zarr Reader class."""

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
        """Set Test class for Tio Zarr Reader."""
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='tio.zarr',
            is_latest=True
        )
        self.attribute1 = Attribute.objects.get(
            name='Max Temperature',
            variable_name='max_temperature')
        self.dataset_attr1 = DatasetAttribute.objects.get(
            dataset=self.dataset,
            attribute=self.attribute1,
            source='max_temperature'
        )
        self.attributes = [self.dataset_attr1]
        self.location_input = DatasetReaderInput.from_point(
            Point(LON_METADATA['min'], LAT_METADATA['min'])
        )
        self.start_date = datetime(2024, 10, 3)
        self.end_date = datetime(2024, 10, 5)
        self.reader = TioZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @patch('gap.models.DataSourceFile.objects.filter')
    def test_read_forecast_data_empty(self, mock_filter):
        """Test for reading forecast data that returns empty."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = TioZarrReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.order_by.return_value.last.return_value = (
            None
        )
        reader.read_forecast_data(start_date, end_date)
        self.assertEqual(reader.xrDatasets, [])

    def test_read_forecast_data(self):
        """Test for reading forecast data."""
        dt1 = datetime(2024, 10, 3, tzinfo=pytz.UTC)
        dt2 = datetime(2024, 10, 5, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values().to_json()
            mock_open.assert_called_once()
            result_data = data_value['data']
            self.assertEqual(len(result_data), 3)
            self.assertIn('max_temperature', result_data[0])

    def test_read_from_bbox(self):
        """Test for reading forecast data using bbox."""
        dt1 = datetime(2024, 10, 3, tzinfo=pytz.UTC)
        dt2 = datetime(2024, 10, 5, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.location_input = DatasetReaderInput.from_bbox(
                [
                    LON_METADATA['min'],
                    LAT_METADATA['min'],
                    LON_METADATA['min'] + LON_METADATA['inc'],
                    LAT_METADATA['min'] + LAT_METADATA['inc']
                ]
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('max_temperature', dataset.data_vars)

    def test_read_from_points(self):
        """Test for reading forecast data using points."""
        dt1 = datetime(2024, 10, 3, tzinfo=pytz.UTC)
        dt2 = datetime(2024, 10, 5, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            p1 = Point(LON_METADATA['min'], LAT_METADATA['min'])
            p2 = Point(
                LON_METADATA['min'] + LON_METADATA['inc'],
                LAT_METADATA['min'] + LAT_METADATA['inc']
            )
            self.reader.location_input = DatasetReaderInput(
                MultiPoint([p1, p2]),
                LocationInputType.LIST_OF_POINT
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('max_temperature', dataset.data_vars)

    def test_read_past_forecast_data(self):
        """Test for reading forecast data."""
        dt1 = datetime(2024, 8, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2024, 8, 2, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values().to_json()
            mock_open.assert_called_once()
            result_data = data_value['data']
            self.assertEqual(len(result_data), 0)

    def test_read_past_forecast_data_using_bbox(self):
        """Test for reading forecast data using bbox."""
        dt1 = datetime(2024, 8, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2024, 8, 2, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.location_input = DatasetReaderInput.from_bbox(
                [
                    LON_METADATA['min'],
                    LAT_METADATA['min'],
                    LON_METADATA['min'] + LON_METADATA['inc'],
                    LAT_METADATA['min'] + LAT_METADATA['inc']
                ]
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('max_temperature', dataset.data_vars)

    def test_read_past_forecast_data_using_points(self):
        """Test for reading forecast data using points."""
        dt1 = datetime(2024, 8, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2024, 8, 2, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            p1 = Point(LON_METADATA['min'], LAT_METADATA['min'])
            p2 = Point(
                LON_METADATA['min'] + LON_METADATA['inc'],
                LAT_METADATA['min'] + LAT_METADATA['inc']
            )
            self.reader.location_input = DatasetReaderInput(
                MultiPoint([p1, p2]),
                LocationInputType.LIST_OF_POINT
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('max_temperature', dataset.data_vars)

    def test_entirely_in_past(self):
        """Test split date range entirely in past."""
        start_date = datetime(2024, 11, 20)
        end_date = datetime(2024, 11, 25)
        now = datetime(2024, 12, 1)
        result = self.reader._split_date_range(start_date, end_date, now)
        self.assertEqual(
            result, {'past': (start_date, end_date), 'future': None}
        )

    def test_entirely_in_future(self):
        """Test split date range entirely in future."""
        start_date = datetime(2024, 12, 10)
        end_date = datetime(2024, 12, 15)
        now = datetime(2024, 12, 1)
        result = self.reader._split_date_range(start_date, end_date, now)
        self.assertEqual(
            result, {'past': None, 'future': (start_date, end_date)}
        )

    def test_split_between_past_and_future(self):
        """Test split date range between past and future."""
        start_date = datetime(2024, 11, 20)
        end_date = datetime(2024, 12, 10)
        now = datetime(2024, 12, 1)
        result = self.reader._split_date_range(start_date, end_date, now)
        self.assertEqual(result, {
            'past': (start_date, now - timedelta(days=1)),
            'future': (now, end_date)
        })

    def test_now_equals_start_date(self):
        """Test split date range now = start_date."""
        start_date = datetime(2024, 12, 1)
        end_date = datetime(2024, 12, 10)
        now = datetime(2024, 12, 1)
        result = self.reader._split_date_range(start_date, end_date, now)
        self.assertEqual(result, {
            'past': None,
            'future': (now, end_date)
        })

    def test_now_equals_end_date(self):
        """Test split date range now = end_date."""
        start_date = datetime(2024, 11, 20)
        end_date = datetime(2024, 12, 1)
        now = datetime(2024, 12, 1)
        result = self.reader._split_date_range(start_date, end_date, now)
        self.assertEqual(result, {
            'past': (start_date, now - timedelta(days=1)),
            'future': (now, now)
        })


class TestHourlyTioZarrReader(TestCase):
    """Unit test for Hourly Tio Zarr Reader class."""

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
        """Set Test class for Tio Zarr Reader."""
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
            store_type=DatasetStore.ZARR
        )
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='tio_hourly.zarr',
            is_latest=True
        )
        self.attribute1 = Attribute.objects.get(
            name='Temperature',
            variable_name='temperature')
        self.dataset_attr1 = DatasetAttribute.objects.get(
            dataset=self.dataset,
            attribute=self.attribute1,
            source='temperature'
        )
        self.attributes = [self.dataset_attr1]
        self.location_input = DatasetReaderInput.from_point(
            Point(LON_METADATA['min'], LAT_METADATA['min'])
        )
        self.start_date = datetime(2025, 4, 24)
        self.end_date = datetime(2025, 4, 28)
        self.reader = TioZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    def test_read_from_bbox(self):
        """Test for reading forecast data using bbox."""
        dt1 = datetime(2025, 4, 24, tzinfo=pytz.UTC)
        dt2 = datetime(2025, 4, 27, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_hourly_open_zarr_dataset()
            self.reader.location_input = DatasetReaderInput.from_bbox(
                [
                    LON_METADATA['min'],
                    LAT_METADATA['min'],
                    LON_METADATA['min'] + LON_METADATA['inc'],
                    LAT_METADATA['min'] + LAT_METADATA['inc']
                ]
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('temperature', dataset.data_vars)
            result = list(data_value.to_csv_stream())
            header = result[0].decode('utf-8')
            self.assertIn('date', header)
            self.assertIn('time', header)
            self.assertIn('lat', header)
            self.assertIn('lon', header)
            self.assertIn('temperature', header)
            result_str = StringIO(header + '\n' + str(result[1]))
            result_df = pd.read_csv(result_str)
            df = result_df[
                (result_df['date'] == '2025-04-24') &
                (result_df['lat'] == -4.65014) &
                (result_df['lon'] == 33.9182)
            ]
            self.assertEqual(len(df), 24)

    def test_read_past_forecast_data(self):
        """Test for reading forecast data."""
        dt1 = datetime(2025, 4, 24, 6, 0, 0, tzinfo=pytz.UTC)
        dt2 = datetime(2025, 4, 27, 1, 0, 0, tzinfo=pytz.UTC)
        self.reader.start_date = dt1
        self.reader.end_date = dt2
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_hourly_open_zarr_dataset()
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values().to_json()
            mock_open.assert_called_once()
            result_data = data_value['data']
            self.assertEqual(len(result_data), 68)
