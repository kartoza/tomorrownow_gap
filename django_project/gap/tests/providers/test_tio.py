# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tomorrow.io Dataset Reader.
"""

from django.test import TestCase
from unittest.mock import patch
from datetime import datetime, timedelta
import pytz
import requests_mock
from django.contrib.gis.geos import Point

from gap.models import (
    Dataset,
    DatasetAttribute,
    DatasetType,
    CastType,
    DatasetStore
)
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.providers.tio import TomorrowIODatasetReader


class TestTomorrowIODatasetReader(TestCase):
    """Test class for Tomorrow io dataset reader."""

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
        """Set test class."""
        TomorrowIODatasetReader.init_provider()
        self.dataset = Dataset.objects.filter(
            provider__name='Tomorrow.io',
            store_type=DatasetStore.EXT_API
        ).first()

        attr = DatasetAttribute.objects.filter(
            source='rainAccumulationSum',
            dataset=self.dataset
        ).first()

        self.attributes = [attr]
        self.location_input = DatasetReaderInput.from_point(
            Point(y=40.7128, x=-74.0060))
        self.start_date = datetime(2023, 7, 1, tzinfo=pytz.UTC)
        self.start_date.replace(microsecond=0)
        self.end_date = datetime(2023, 7, 10, tzinfo=pytz.UTC)
        self.end_date.replace(microsecond=0)

        self.reader = TomorrowIODatasetReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @requests_mock.Mocker()
    @patch('os.environ.get', return_value='dummy_api_key')
    def test_read_historical_data(self, mock_request, mock_env):
        """Test read historical data."""
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startTime': '2023-07-01T00:00:00Z',
                            'values': {
                                'rainAccumulationSum': 5.0
                            }
                        }
                    ]
                }]
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/historical?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader.read_historical_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 1)
        self.assertEqual(self.reader.results[0].values['total_rainfall'], 5.0)
        # mock 400 error
        mock_response = {
            'type': 'Unknown',
            'message': 'Test error'
        }
        self.reader.errors = None
        self.reader.results = []
        mock_request.post(
            'https://api.tomorrow.io/v4/historical?apikey=dummy_api_key',
            json=mock_response, status_code=400
        )

        self.reader.read_historical_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 0)
        self.assertEqual(len(self.reader.errors), 1)

    @requests_mock.Mocker()
    @patch('os.environ.get', return_value='dummy_api_key')
    def test_read_forecast_data(self, mock_request, mock_env):
        """Test read forecast data."""
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startTime': '2023-07-01T00:00:00Z',
                            'values': {
                                'rainAccumulationSum': 10.0
                            }
                        }
                    ]
                }],
                'warnings': ['test-warnings']
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/timelines?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader.read_forecast_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 1)
        self.assertEqual(
            self.reader.results[0].values['total_rainfall'], 10.0)
        self.assertEqual(len(self.reader.warnings), 1)
        # test get_data_values
        reader_value = self.reader.get_data_values()
        self.assertEqual(
            reader_value.location_input.point, self.location_input.point)
        self.assertEqual(len(reader_value._val), 1)
        # mock 400 error
        mock_response = {
            'type': 'Unknown',
            'message': 'Test error'
        }
        self.reader.errors = []
        self.reader.results = []
        mock_request.post(
            'https://api.tomorrow.io/v4/timelines?apikey=dummy_api_key',
            json=mock_response, status_code=400
        )

        self.reader.read_forecast_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 0)
        self.assertEqual(len(self.reader.errors), 1)
        # test get_data_values
        reader_value = self.reader.get_data_values()
        self.assertEqual(
            reader_value.location_input.point, self.location_input.point)
        self.assertEqual(len(reader_value._val), 0)

    @requests_mock.Mocker()
    @patch('os.environ.get', return_value='dummy_api_key')
    def test_read_ltn_data(self, mock_request, mock_env):
        """Test read LTN data."""
        self.reader.dataset = Dataset.objects.filter(
            provider__name='Tomorrow.io',
            type__name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
        ).first()
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startDate': '07-01',
                            'values': {
                                'rainAccumulationSum': 2.0
                            }
                        }
                    ]
                }]
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/historical/'
            'normals?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader._read_ltn_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 1)
        self.assertEqual(self.reader.results[0].values['total_rainfall'], 2.0)
        # mock 400 error
        mock_response = {
            'type': 'Unknown',
            'message': 'Test error'
        }
        self.reader.errors = []
        self.reader.results = []
        mock_request.post(
            'https://api.tomorrow.io/v4/historical/'
            'normals?apikey=dummy_api_key',
            json=mock_response, status_code=400
        )

        self.reader._read_ltn_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 0)
        self.assertEqual(len(self.reader.errors), 1)
        # test LTN returns 02-29
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startDate': '02-29',
                            'values': {
                                'rainAccumulationSum': 2.0
                            }
                        }
                    ]
                }]
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/historical/'
            'normals?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader._read_ltn_data(self.start_date, self.end_date)
        # skip the result since requested year is not leap year
        self.assertEqual(len(self.reader.results), 0)

    @patch.object(TomorrowIODatasetReader, 'read_historical_data')
    @patch.object(TomorrowIODatasetReader, 'read_forecast_data')
    @patch.object(TomorrowIODatasetReader, '_read_ltn_data')
    def test_read_ltn_request(
        self, mock_read_ltn_data, mock_read_forecast_data,
        mock_read_historical_data):
        """Test read() method that calls ltn request."""
        dt_now = datetime.now(tz=pytz.UTC)
        dt_now.replace(microsecond=0)
        self.reader.start_date = dt_now - timedelta(days=10)
        self.reader.end_date = dt_now
        self.reader.dataset.type = DatasetType(
            type=CastType.HISTORICAL,
            name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE)

        # Call the read method
        with patch('gap.providers.tio.datetime') as mock_datetime:
            mock_datetime.now.return_value = dt_now
            self.reader.read()

        # Check that the correct method is called for LTN
        mock_read_ltn_data.assert_called_once()
        self.assertEqual(
            mock_read_ltn_data.call_args[0][0], self.reader.start_date)
        self.assertEqual(
            mock_read_ltn_data.call_args[0][1], self.reader.end_date)
        mock_read_historical_data.assert_not_called()
        mock_read_forecast_data.assert_not_called()

    @patch.object(TomorrowIODatasetReader, 'read_historical_data')
    @patch.object(TomorrowIODatasetReader, 'read_forecast_data')
    @patch.object(TomorrowIODatasetReader, '_read_ltn_data')
    def test_read_historical_and_forecast_data(
        self, mock_read_ltn_data, mock_read_forecast_data,
        mock_read_historical_data):
        """Test read() method that calls historical and forecast."""
        dt_now = datetime.now(tz=pytz.UTC)
        dt_now.replace(microsecond=0)
        self.reader.dataset.type = DatasetType(
            type=CastType.HISTORICAL, name='TestHistorical')
        self.reader.start_date = dt_now - timedelta(days=10)
        self.reader.end_date = dt_now  # Ensure end_date is after max_date

        # Call the read method
        with patch('gap.providers.tio.datetime') as mock_datetime:
            mock_datetime.now.return_value = dt_now
            self.reader.read()

        # Check that the correct methods are called
        max_date = dt_now - timedelta(days=7)
        mock_read_historical_data.assert_called_once()
        self.assertEqual(
            mock_read_historical_data.call_args[0][0], self.reader.start_date)
        self.assertEqual(mock_read_historical_data.call_args[0][1], max_date)
        mock_read_forecast_data.assert_called_once()
        self.assertEqual(
            mock_read_forecast_data.call_args[0][0],
            max_date + timedelta(days=1))
        self.assertEqual(
            mock_read_forecast_data.call_args[0][1], self.reader.end_date)
        mock_read_ltn_data.assert_not_called()

    @patch.object(TomorrowIODatasetReader, 'read_historical_data')
    @patch.object(TomorrowIODatasetReader, 'read_forecast_data')
    @patch.object(TomorrowIODatasetReader, '_read_ltn_data')
    def test_read_forecast_data_only(
        self, mock_read_ltn_data, mock_read_forecast_data,
        mock_read_historical_data):
        """Test read() method that calls forecast only."""
        dt_now = datetime.now(tz=pytz.UTC)
        dt_now.replace(microsecond=0)
        self.reader.end_date = dt_now
        self.reader.dataset.type = DatasetType(
            type=CastType.FORECAST, name='TestForecast')
        self.reader.start_date = dt_now - timedelta(days=7)

        # Call the read method
        with patch('gap.providers.tio.datetime') as mock_datetime:
            mock_datetime.now.return_value = dt_now
            self.reader.read()

        # Check that the correct method is called
        mock_read_forecast_data.assert_called_once()
        self.assertEqual(
            mock_read_forecast_data.call_args[0][0],
            dt_now - timedelta(days=6))
        self.assertEqual(
            mock_read_forecast_data.call_args[0][1], self.reader.end_date)
        mock_read_historical_data.assert_not_called()
        mock_read_ltn_data.assert_not_called()
