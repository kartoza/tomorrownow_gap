# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Partitions functions.
"""

from mock import patch
import pandas as pd

from dcas.tests.base import DCASPipelineBaseTest
from dcas.partitions import (
    _merge_partition_gdd_config,
    process_partition_farm_registry,
    process_partition_seasonal_precipitation,
    process_partition_other_params,
    process_partition_growth_stage_precipitation
)


class DCASPartitionsTest(DCASPipelineBaseTest):
    """DCAS Partitions test case."""

    def test_merge_partition_gdd_config(self):
        """Test merge_partition_gdd_config."""
        df = pd.DataFrame({
            'crop_id': [2, 10],  # Maize and Cassava
            'config_id': [self.default_config.id, self.default_config.id]
        })
        df = _merge_partition_gdd_config(df)
        self.assertIn('gdd_base', df.columns)
        self.assertIn('gdd_cap', df.columns)
        expected_df = pd.DataFrame({
            'crop_id': [2, 10],
            'config_id': [self.default_config.id, self.default_config.id],
            'gdd_base': [10, 12],
            'gdd_cap': [35, 35]
        })
        expected_df['gdd_base'] = expected_df['gdd_base'].astype('float64')
        expected_df['gdd_cap'] = expected_df['gdd_cap'].astype('float64')
        pd.testing.assert_frame_equal(df, expected_df)

    @patch('dcas.partitions.read_grid_crop_data')
    def test_process_partition_farm_registry(self, mock_read_grid_data):
        """Test process_partition_farm_registry."""
        df = pd.DataFrame({
            'crop_id': [2, 10],
            'grid_id': [1, 2],
            'crop_stage_type_id': [1, 2],
            'planting_date_epoch': [5, 6],
            'farm_id': [3, 4],
            'growth_stage_id': [1, 1],
            'grid_crop_key': ['2_1_1', '10_2_2']
        })

        mock_read_grid_data.return_value = pd.DataFrame({
            'crop_id': [2, 10, 7],
            'grid_id': [1, 2, 7],
            'crop_stage_type_id': [1, 2, 7],
            'planting_date_epoch': [5, 6, 7],
            'temperature': [9, 8, 7],
            '__null_dask_index__': [0, 1, 2],
            'planting_date': ['2025-01-01', '2025-01-05', '2025-01-05'],
            'grid_crop_key': ['2_1_1', '10_2_2', '7_7_7']
        })

        result_df = process_partition_farm_registry(
            df, 'test.parquet', {1: 'growth_stage_a'}
        )
        mock_read_grid_data.assert_called_once()
        self.assertEqual(result_df.shape[0], 2)
        pd.testing.assert_series_equal(
            result_df['farm_id'],
            pd.Series([3, 4], name='farm_id')
        )
        pd.testing.assert_series_equal(
            result_df['grid_id'],
            pd.Series([1, 2], name='grid_id')
        )
        pd.testing.assert_series_equal(
            result_df['crop_id'],
            pd.Series([2, 10], name='crop_id')
        )
        pd.testing.assert_series_equal(
            result_df['growth_stage'],
            pd.Series(
                ['growth_stage_a', 'growth_stage_a'],
                name='growth_stage'
            )
        )
        pd.testing.assert_series_equal(
            result_df['temperature'],
            pd.Series([9, 8], name='temperature')
        )

    @patch('dcas.partitions.read_grid_data')
    def test_process_partition_seasonal_precipitation(
        self, mock_read_grid_data
    ):
        """Test process_partition_seasonal_precipitation."""
        epoch_list = [1, 2]
        df = pd.DataFrame({
            'grid_id': [1, 2, 1],
            'planting_date_epoch': [1, 2, 1],
        })

        mock_read_grid_data.return_value = pd.DataFrame({
            'grid_id': [1, 2],
            'total_rainfall_1': [10, 12],
            'total_rainfall_2': [13, 14]
        })

        result_df = process_partition_seasonal_precipitation(
            df, 'test.parquet', epoch_list
        )
        mock_read_grid_data.assert_called_once()
        self.assertEqual(result_df.shape[0], 3)
        pd.testing.assert_series_equal(
            result_df['grid_id'],
            pd.Series([1, 2, 1], name='grid_id')
        )
        pd.testing.assert_series_equal(
            result_df['seasonal_precipitation'],
            pd.Series(
                [23, 14, 23],
                name='seasonal_precipitation',
                dtype='float64'
            )
        )

    @patch('dcas.partitions.read_grid_data')
    def test_process_partition_other_params(self, mock_read_grid_data):
        """Test process_partition_other_params."""
        df = pd.DataFrame({
            'grid_id': [1, 2]
        })

        mock_read_grid_data.return_value = pd.DataFrame({
            'grid_id': [1, 2],
            'temperature': [10, 12],
            'humidity': [13, 14],
            'p_pet': [15, 16]
        })

        result_df = process_partition_other_params(
            df, 'test.parquet'
        )
        mock_read_grid_data.assert_called_once()
        self.assertEqual(result_df.shape[0], 2)
        pd.testing.assert_series_equal(
            result_df['grid_id'],
            pd.Series([1, 2], name='grid_id')
        )
        pd.testing.assert_series_equal(
            result_df['temperature'],
            pd.Series([10, 12], name='temperature')
        )
        pd.testing.assert_series_equal(
            result_df['humidity'],
            pd.Series([13, 14], name='humidity')
        )
        pd.testing.assert_series_equal(
            result_df['p_pet'],
            pd.Series([15, 16], name='p_pet')
        )

    @patch('dcas.partitions.read_grid_data')
    def test_process_partition_growth_precipitation(
        self, mock_read_grid_data
    ):
        """Test process_partition_growth_stage_precipitation."""
        epoch_list = [1, 2]
        df = pd.DataFrame({
            'grid_id': [1, 2, 1],
            'growth_stage_start_date': [1, 2, 1]
        })

        mock_read_grid_data.return_value = pd.DataFrame({
            'grid_id': [1, 2],
            'total_rainfall_1': [10, 12],
            'total_rainfall_2': [13, 14]
        })

        result_df = process_partition_growth_stage_precipitation(
            df, 'test.parquet', epoch_list
        )
        mock_read_grid_data.assert_called_once()
        self.assertEqual(result_df.shape[0], 3)
        pd.testing.assert_series_equal(
            result_df['grid_id'],
            pd.Series([1, 2, 1], name='grid_id')
        )
        pd.testing.assert_series_equal(
            result_df['growth_stage_precipitation'],
            pd.Series(
                [23, 14, 23],
                name='growth_stage_precipitation',
                dtype='float64'
            )
        )
