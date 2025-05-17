# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytz
from django.contrib.gis.geos import Point
from django.test import TestCase
import xarray as xr
import numpy as np
import pandas as pd

from gap.models import (
    DatasetAttribute,
    Dataset,
    DataSourceFile,
    DatasetStore
)
from gap.providers.tio import (
    TomorrowIODatasetReader,
    TioZarrReader,
    TioZarrReaderValue
)
from gap.utils.reader import (
    DatasetReaderInput,
    DatasetTimelineValue
)
from spw.factories import (
    RModelFactory
)
from spw.generator.gap_input import GapInput
from spw.generator.main import (
    SPWOutput,
    calculate_from_point,
    calculate_from_point_attrs,
    VAR_MAPPING_REVERSE
)
from spw.models import RModelExecutionLog, RModelExecutionStatus


class TestSPWOutput(TestCase):
    """Unit test for SPWOutput class."""

    def setUp(self):
        """Set the test class."""
        self.point = Point(y=1.0, x=1.0)
        self.input_data = {
            'temperature': [20.5],
            'pressure': [101.3],
            'humidity': 45,
            'metadata': 'some metadata'
        }
        self.expected_data = {
            'temperature': 20.5,
            'pressure': 101.3,
            'humidity': 45
        }

    def test_initialization(self):
        """Test initialization of SPWOutput class."""
        spw_output = SPWOutput(self.point, self.input_data)

        self.assertEqual(spw_output.point, self.point)
        self.assertEqual(
            spw_output.data.temperature, self.expected_data['temperature'])
        self.assertEqual(
            spw_output.data.pressure, self.expected_data['pressure'])
        self.assertEqual(
            spw_output.data.humidity, self.expected_data['humidity'])

    def test_input_data_without_metadata(self):
        """Test initialization of SPWOutput class without metadata."""
        input_data = {
            'temperature': [20.5],
            'pressure': [101.3],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'][0])
        self.assertEqual(spw_output.data.pressure, input_data['pressure'][0])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])

    def test_input_data_with_single_element_list(self):
        """Test initialization of SPWOutput class for single element."""
        input_data = {
            'temperature': [20.5],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'][0])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])

    def test_input_data_with_multiple_element_list(self):
        """Test initialization of SPWOutput class for list."""
        input_data = {
            'temperature': [20.5, 21.0],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])


class TestSPWFetchDataFunctions(TestCase):
    """Test SPW fetch data functions."""

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
        """Set test fetch data functions."""
        TomorrowIODatasetReader.init_provider()
        self.dataset = Dataset.objects.filter(
            provider__name='Tomorrow.io'
        ).first()
        self.location_input = DatasetReaderInput.from_point(Point(0, 0))
        attr1 = DatasetAttribute.objects.filter(
            source='rainAccumulationSum',
            dataset=self.dataset
        ).first()
        attr2 = DatasetAttribute.objects.filter(
            source='evapotranspirationSum',
            dataset=self.dataset
        ).first()
        self.attrs = [attr1, attr2]
        self.dt_now = datetime.now(tz=pytz.UTC).replace(microsecond=0)
        self.start_dt = self.dt_now - timedelta(days=10)
        self.end_dt = self.dt_now

    @patch.object(TomorrowIODatasetReader, 'read')
    @patch.object(TomorrowIODatasetReader, 'get_raw_results')
    def test_fetch_timelines_data(self, mocked_results, mocked_read):
        """Test fetch timelines data for SPW."""
        mocked_read.side_effect = MagicMock()
        mocked_results.return_value = [
            DatasetTimelineValue(
                datetime(2023, 7, 20),
                {
                    'total_evapotranspiration_flux': 10,
                    'total_rainfall': 5
                },
                self.location_input.point
            )
        ]
        gap_input = GapInput(0, 0, self.dt_now)
        result = gap_input._fetch_timelines_data()
        expected_result = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'precipitationProbability': 0,
                'rainAccumulationSum': 5,
                'temperatureMax': 0,
                'temperatureMin': 0
            }
        }
        self.assertEqual(result, expected_result)

    @patch.object(TomorrowIODatasetReader, 'read')
    @patch.object(TomorrowIODatasetReader, 'get_raw_results')
    def test_fetch_ltn_data(self, mocked_results, mocked_read):
        """Test fetch ltn data for SPW."""
        mocked_read.side_effect = MagicMock()
        mocked_results.return_value = [
            DatasetTimelineValue(
                datetime(2023, 7, 20),
                {'total_evapotranspiration_flux': 8, 'total_rainfall': 3},
                self.location_input.point
            )
        ]
        # Initial historical data
        historical_dict = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5
            }
        }
        gap_input = GapInput(0, 0, self.dt_now)
        result = gap_input._fetch_ltn_data(historical_dict)
        expected_result = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'LTNPET': 8,
                'LTNPrecip': 3
            }
        }
        self.assertEqual(result, expected_result)

    @patch.object(TioZarrReader, 'read')
    @patch.object(TioZarrReader, 'get_data_values')
    def test_fetch_from_zarr(self, mocked_results, mocked_read):
        """Test fetch data for SPW from zarr."""
        gap_input = GapInput(0, 0, self.dt_now)
        dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        attrs = DatasetAttribute.objects.filter(
            dataset=dataset,
            source__in=[
                'total_evapotranspiration_flux',
                'total_rainfall',
                'max_temperature',
                'min_temperature',
                'precipitation_probability'
            ]
        )
        # create DataSourceFile
        DataSourceFile.objects.create(
            name='test.zarr',
            format='ZARR',
            start_date_time=self.start_dt,
            end_date_time=self.end_dt,
            created_on=self.start_dt,
            dataset=dataset,
            is_latest=True,
        )

        mocked_read.side_effect = MagicMock()
        new_lat = [0]
        new_lon = [0]
        forecast_date_array = pd.date_range('2023-07-14', periods=16)
        empty_shape = (16, len(new_lat), len(new_lon))
        data_vars = {
            'total_evapotranspiration_flux': (
                ('date', 'lat', 'lon'),
                np.full(empty_shape, 5)
            ),
            'precipitation_probability': (
                ('date', 'lat', 'lon'),
                np.full(empty_shape, 10)
            ),
            'total_rainfall': (
                ('date', 'lat', 'lon'),
                np.full(empty_shape, 15)
            ),
            'max_temperature': (
                ('date', 'lat', 'lon'),
                np.full(empty_shape, 20)
            ),
            'min_temperature': (
                ('date', 'lat', 'lon'),
                np.full(empty_shape, 15)
            )
        }
        xr_ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'date': ('date', forecast_date_array),
                'lat': ('lat', new_lat),
                'lon': ('lon', new_lon)
            }
        )
        mocked_results.return_value = TioZarrReaderValue(
            xr_ds, self.location_input, attrs,
            self.start_dt, None, None
        )
        result = gap_input._read_forecast_from_zarr()
        expected_result = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 5,
                'precipitationProbability': 10,
                'rainAccumulationSum': 15,
                'temperatureMax': 20,
                'temperatureMin': 15
            }
        }
        # find 07-20 in result
        self.assertIn('07-20', result)
        # check if the values are correct
        self.assertEqual(result['07-20'], expected_result['07-20'])


class TestSPWGenerator(TestCase):
    """Test SPW Generator functions."""

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
        """Set the test class."""
        self.dt_now = datetime.now(tz=pytz.UTC).replace(microsecond=0)
        self.location_input = DatasetReaderInput.from_point(Point(0, 0))
        self.r_model = RModelFactory.create(name='test')

    @patch('spw.generator.main.datetime')
    @patch('spw.generator.main.execute_spw_model')
    @patch('spw.generator.gap_input.GapInput._fetch_timelines_data')
    @patch('spw.generator.gap_input.GapInput._fetch_ltn_data')
    def test_calculate_from_point(
            self, mock_fetch_ltn_data, mock_fetch_timelines_data,
            mock_execute_spw_model, mock_now):
        """Test calculate_from_point function."""
        mock_now.now.return_value = datetime(
            2023, 7, 20, 0, 0, 0, tzinfo=pytz.UTC
        )
        mock_fetch_ltn_data.return_value = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'LTNPET': 8,
                'LTNPrecip': 3
            }
        }
        mock_fetch_timelines_data.return_value = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5
            }
        }
        r_data = {
            'metadata': {
                'test': 'abcdef'
            },
            'goNoGo': ['Do not plant Tier 1a'],
            'nearDaysLTNPercent': [10.0],
            'nearDaysCurPercent': [60.0],
        }
        mock_execute_spw_model.return_value = (True, r_data)

        output, historical_dict = calculate_from_point(
            self.location_input.point
        )
        mock_fetch_ltn_data.assert_called_once()
        mock_fetch_timelines_data.assert_called_once()
        mock_execute_spw_model.assert_called_once()
        self.assertEqual(output.data.goNoGo, r_data['goNoGo'][0])
        self.assertEqual(
            output.data.nearDaysLTNPercent, r_data['nearDaysLTNPercent'][0])
        self.assertEqual(
            output.data.nearDaysCurPercent, r_data['nearDaysCurPercent'][0])
        # find RModelExecutionLog
        log = RModelExecutionLog.objects.filter(
            model=self.r_model,
            location_input=self.location_input.point
        ).first()
        self.assertTrue(log)
        self.assertTrue(log.input_file)
        self.assertTrue(log.output)
        self.assertEqual(log.status, RModelExecutionStatus.SUCCESS)


class TestSPWAttrs(TestCase):
    """Test fetch attributes for SPW Tio."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def test_attrs(self):
        """Test to ensure attrs is correct for Tio API Call."""
        attrs = calculate_from_point_attrs()
        for k, v in VAR_MAPPING_REVERSE.items():
            self.assertTrue(attrs.filter(source=k).exists())
