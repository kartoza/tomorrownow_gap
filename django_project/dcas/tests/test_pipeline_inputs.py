# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline Inputs.
"""

import mock
import xarray as xr
import numpy as np
import pandas as pd
import datetime
from django.test import TestCase

from gap.providers.tio import TioZarrReaderValue
from dcas.inputs import DCASPipelineInput


class DCASPipelineInputsTest(TestCase):
    """DCAS Pipeline Inputs test case."""

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
        """Set the DCASPipelineInputsTest data."""
        # Create a mock xarray Dataset
        self.dates = pd.date_range(
            datetime.date(2025, 1, 15),
            datetime.date(2025, 1, 15) + datetime.timedelta(days=3),
            freq='d'
        )
        lat = np.linspace(0, 10, 6)  # [0, 2, 4, 6, 8, 10]
        lon = np.linspace(0, 10, 6)  # [0, 2, 4, 6, 8, 10]
        data = np.array([
            [10, 12, 14, 16, 18, 20],  # lat=0
            [20, 22, 24, 26, 28, 30],  # lat=2
            [30, 32, 34, 36, 38, 40],  # lat=4
            [40, 42, 44, 46, 48, 50],  # lat=6
            [50, 52, 54, 56, 58, 60],  # lat=8
            [60, 62, 64, 66, 68, 70],  # lat=10
        ])
        temperature = [data, data, data, data]

        self.ds = xr.Dataset(
            {
                "temperature": (["date", "lat", "lon"], temperature),
                "total_rainfall": (["date", "lat", "lon"], temperature)
            },
            coords={
                "date": self.dates,
                "lat": lat,
                "lon": lon,
            }
        )

        self.input = DCASPipelineInput(datetime.date(2025, 1, 15))
        self.input.setup(datetime.date(2025, 1, 10))

    def test_get_values_at_points(self):
        """Test points interpolation from xarray Dataset."""
        epoch = int(self.dates[0].timestamp())

        # exact points
        in_lat = pd.Series([2, 4])
        in_lon = pd.Series([2, 4])
        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature'], {
                'temperature': 'temperature'
            },
            in_lat,
            in_lon,
            self.dates[0],
            [0, 1]
        )
        expected_results = [22, 34]
        np.testing.assert_allclose(
            vap_df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )

        # sel with nearest will result to 2, 6, 10
        in_lat = pd.Series([1, 5, 9])
        in_lon = pd.Series([1, 5, 9])

        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature'], {
                'temperature': 'temperature'
            },
            in_lat,
            in_lon,
            self.dates[0],
            [0, 1, 2],
            tolerance=1
        )

        self.assertIn(f'temperature_{epoch}', vap_df.columns)
        expected_results = [22, 46, 70]
        np.testing.assert_allclose(
            vap_df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )

        # using large diff
        in_lat = pd.Series([-2, 15])
        in_lon = pd.Series([-2, 15])
        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature'], {
                'temperature': 'temperature'
            },
            in_lat,
            in_lon,
            self.dates[0],
            [0, 1]
        )
        self.assertTrue(vap_df.isna().all().all())

    def test_multiple_attributes(self):
        """Test selection with multiple attributes."""
        # sel with nearest will result to 2, 6, 10
        in_lat = pd.Series([1, 5, 9])
        in_lon = pd.Series([1, 5, 9])

        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature', 'total_rainfall'], {
                'temperature': 'temperature',
                'total_rainfall': 'total_rainfall'
            },
            in_lat,
            in_lon,
            self.dates[0],
            [0, 1, 2],
            tolerance=1
        )

        epoch = int(self.dates[0].timestamp())
        self.assertIn(f'temperature_{epoch}', vap_df.columns)
        self.assertIn(f'total_rainfall_{epoch}', vap_df.columns)
        expected_results = [22, 46, 70]
        np.testing.assert_allclose(
            vap_df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )
        np.testing.assert_allclose(
            vap_df[f'total_rainfall_{epoch}'].values, expected_results,
            atol=1e-5
        )

    def test_generate_random(self):
        """Test generate random data."""
        grid_list = [1, 2, 3, 4, 5]
        df = pd.DataFrame({
            'grid_id': grid_list
        }, index=grid_list)

        df = self.input._generate_random(df)

        epoch1 = self.input.historical_epoch[0]
        epoch2 = self.input.historical_epoch[-1]
        self.assertIn(f'max_temperature_{epoch1}', df.columns)
        self.assertIn(f'max_temperature_{epoch2}', df.columns)
        self.assertIn(f'max_humidity_{epoch2}', df.columns)
        self.assertIn(f'precipitation_{epoch2}', df.columns)
        self.assertEqual(len(df[f'max_temperature_{epoch1}']), len(grid_list))

    @mock.patch('dcas.inputs.TioZarrReader.read')
    @mock.patch('dcas.inputs.TioZarrReader.get_data_values')
    def test_download_data(self, mock_data_values, mock_read):
        """Test download data."""
        mock_data_values.return_value = TioZarrReaderValue(
            self.ds,
            None,
            [],
            None,
            None,
            None
        )

        self.input._download_data(
            ['temperature'],
            [0, 2, 4, 6],
            self.input.historical_dates[0],
            self.input.historical_dates[-1],
            '/tmp/test.nc'
        )

        mock_read.assert_called_once()
        mock_data_values.assert_called_once()

    @mock.patch('xarray.open_dataset')
    def test_merge_data(self, mock_read):
        """Test merge data."""
        grid_list = [1, 2]
        grid_df = pd.DataFrame({
            'grid_id': grid_list,
            'lat': [2, 4],
            'lon': [2, 4]
        }, index=grid_list)

        mock_read.return_value = self.ds

        df = self.input.merge_dataset(
            'test.nc',
            ['temperature'],
            {
                'temperature': 'temperature'
            },
            self.input.historical_dates,
            grid_df
        )

        expected_results = [22, 34]
        epoch = self.input.historical_epoch[-1]
        np.testing.assert_allclose(
            df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )
        # no date from mock ds should have all na
        epoch = self.input.historical_epoch[0]
        self.assertTrue(df[f'temperature_{epoch}'].isna().all())

    @mock.patch('xarray.open_dataset')
    def test_merge_data_with_invalid_point(self, mock_read):
        """Test merge data."""
        grid_list = [1, 2]
        grid_df = pd.DataFrame({
            'grid_id': grid_list,
            'lat': [-11, 4],
            'lon': [-11, 4]
        }, index=grid_list)

        mock_read.return_value = self.ds

        df = self.input.merge_dataset(
            'test.nc',
            ['temperature'],
            {
                'temperature': 'temperature'
            },
            self.input.historical_dates,
            grid_df
        )

        expected_results = [np.nan, 34]
        epoch = self.input.historical_epoch[-1]
        np.testing.assert_allclose(
            df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )
