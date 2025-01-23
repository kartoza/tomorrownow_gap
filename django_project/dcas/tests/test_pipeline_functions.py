# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline functions.
"""

import numpy as np
import pandas as pd
from mock import patch
from datetime import datetime, timedelta

from dcas.tests.base import DCASPipelineBaseTest
from dcas.functions import (
    calculate_growth_stage, get_last_message_date,
    filter_messages_by_weeks
)


def set_cache_dummy(cache_key, growth_stage_matrix, timeout):
    """Set cache mock."""
    pass


class DCASPipelineFunctionTest(DCASPipelineBaseTest):
    """DCAS Pipeline functions test case."""

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_empty_dict(self, mock_cache):
        """Test calculate_growth_stage from empty_dict."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 9999,
            'crop_stage_type_id': 9999,
            'prev_growth_stage_id': 111,
            'prev_growth_stage_start_date': 111,
            'gdd_sum_123': 9999,
            'config_id': 9999
        }
        epoch_list = [123]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 111)
        self.assertEqual(row['growth_stage_start_date'], 111)

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_no_change(self, mock_cache):
        """Test calculate_growth_stage no change from previous."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'prev_growth_stage_id': 2,
            'prev_growth_stage_start_date': 111,
            'gdd_sum_123': 440,
            'config_id': 1
        }
        epoch_list = [123]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 111)

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_find_using_threshold(self, mock_cache):
        """Test calculate_growth_stage no change from previous."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 420,
            'gdd_sum_124': 440,
            'gdd_sum_125': 450,
            'gdd_sum_126': 490,
            'config_id': 1
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

        # growth start date equals to the last item
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 410,
            'gdd_sum_124': 400,
            'gdd_sum_125': 420,
            'gdd_sum_126': 440,
            'config_id': 1
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 126)

        # growth start date equals to the only data
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 450,
            'config_id': 1
        }
        epoch_list = [
            123
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 123)

        # growth start date equals to planting date
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 124,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': np.nan,
            'gdd_sum_124': 440,
            'config_id': 1
        }
        epoch_list = [
            123,
            124
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_na_value(self, mock_cache):
        """Test calculate_growth_stage no change from previous."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': pd.NA,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 420,
            'gdd_sum_124': 440,
            'gdd_sum_125': 450,
            'gdd_sum_126': 490,
            'config_id': 1
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': np.nan,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 420,
            'gdd_sum_124': 440,
            'gdd_sum_125': 450,
            'gdd_sum_126': 490,
            'config_id': 1
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

    @patch("dcas.functions.read_grid_crop_data")
    def test_get_last_message_date_exists(self, mock_read_grid_crop_data):
        """
        Test when a message exists in history.

        It should return the latest timestamp among all message columns.
        """
        now = datetime.now()
        mock_data = pd.DataFrame({
            'farm_id': [1, 1, 1, 2, 2, 3],
            'crop_id': [100, 100, 100, 101, 101, 102],
            'message': ['MSG1', 'MSG2', 'MSG1', 'MSG3', 'MSG1', 'MSG4'],
            'message_2': [None, 'MSG1', None, None, 'MSG3', None],
            'message_3': [None, None, 'MSG1', None, None, None],
            'message_4': [None, None, None, None, None, None],
            'message_5': [None, None, None, 'MSG1', None, 'MSG4'],
            'message_date': [
                now - timedelta(days=15),  # MSG1 - Old
                now - timedelta(days=10),  # MSG2
                now - timedelta(days=5),   # MSG1 - More recent
                now - timedelta(days=12),  # MSG3
                now - timedelta(days=3),   # MSG1 - Most recent
                now - timedelta(days=20)   # MSG4 - Oldest
            ]
        })

        mock_read_grid_crop_data.return_value = mock_data

        # Latest MSG1 should be at index 4 (3 days ago)
        result = get_last_message_date(2, 101, "MSG1", "/fake/path")
        assert result == mock_data['message_date'].iloc[4]

        # Latest MSG3 should be at index 3 (12 days ago)
        result = get_last_message_date(2, 101, "MSG3", "/fake/path")
        assert result == mock_data['message_date'].iloc[4]

        # Latest MSG2 should be at index 1 (10 days ago)
        result = get_last_message_date(1, 100, "MSG2", "/fake/path")
        assert result == mock_data['message_date'].iloc[1]

        # Latest MSG1 for farm 1, crop 100 should be at index 2 (5 days ago)
        result = get_last_message_date(1, 100, "MSG1", "/fake/path")
        assert result == mock_data['message_date'].iloc[2]

        # MSG5 exists only once, at index 3 (12 days ago)
        result = get_last_message_date(2, 101, "MSG5", "/fake/path")
        assert result is None  # No MSG5 found

    @patch("dcas.functions.read_grid_crop_data")
    def test_get_last_message_date_not_exists(self, mock_read_grid_crop_data):
        """Test when the message does not exist in history."""
        mock_data = pd.DataFrame({
            'farm_id': [1, 1, 2],
            'crop_id': [100, 100, 101],
            'message': ['MSG2', 'MSG3', 'MSG4'],
            'message_2': [None, None, None],
            'message_3': [None, None, None],
            'message_4': [None, None, None],
            'message_5': [None, None, None],
            'message_date': [
                pd.Timestamp(datetime.now() - timedelta(days=10)),
                pd.Timestamp(datetime.now() - timedelta(days=5)),
                pd.Timestamp(datetime.now() - timedelta(days=3))
            ]
        })
        mock_read_grid_crop_data.return_value = mock_data

        result = get_last_message_date(1, 100, "MSG1", "/fake/path")
        self.assertIsNone(result)

    @patch("dcas.functions.read_grid_crop_data")
    def test_get_last_message_date_multiple_messages(
        self,
        mock_read_grid_crop_data
    ):
        """
        Test when the same message appears multiple times.

        And should return the most recent timestamp.
        """
        mock_data = pd.DataFrame({
            'farm_id': [1, 1, 1],
            'crop_id': [100, 100, 100],
            'message': ['MSG1', 'MSG1', 'MSG1'],
            'message_2': [None, None, None],
            'message_3': [None, None, None],
            'message_4': [None, None, None],
            'message_5': [None, None, None],
            'message_date': [
                pd.Timestamp(datetime.now() - timedelta(days=15)),
                pd.Timestamp(datetime.now() - timedelta(days=7)),
                pd.Timestamp(datetime.now() - timedelta(days=2))
            ]
        })
        mock_read_grid_crop_data.return_value = mock_data

        result = get_last_message_date(1, 100, "MSG1", "/fake/path")
        self.assertEqual(result, mock_data['message_date'].iloc[2])

    @patch("dcas.functions.get_last_message_date")
    def test_filter_messages_by_weeks(self, mock_get_last_message_date):
        """Test filtering messages based on the time constraint (weeks)."""
        test_weeks = 2  # Remove messages sent within the last 2 weeks
        current_date = pd.Timestamp(datetime.now())

        df = pd.DataFrame({
            'farm_id': [1, 2, 3],
            'crop_id': [100, 200, 300],
            'message': ['MSG1', 'MSG2', 'MSG3'],
            'message_2': [None, None, None],
            'message_3': [None, None, None],
            'message_4': [None, None, None],
            'message_5': [None, None, None],
        })

        # Simulating last message dates for each row
        mock_get_last_message_date.side_effect = [
            current_date - timedelta(weeks=1),  # Should be removed
            current_date - timedelta(weeks=3),  # Should stay
            None  # No history, should stay
        ]

        filtered_df = filter_messages_by_weeks(df, "/fake/path", test_weeks)

        # Assert that the correct messages were removed
        self.assertIsNone(filtered_df.loc[0, 'message'])  # Removed
        self.assertEqual(filtered_df.loc[1, 'message'], 'MSG2')  # Kept
        self.assertEqual(filtered_df.loc[2, 'message'], 'MSG3')  # Kept
