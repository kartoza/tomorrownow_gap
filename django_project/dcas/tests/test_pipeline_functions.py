# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline functions.
"""

import numpy as np
import pandas as pd
from mock import patch, MagicMock
from datetime import datetime, timedelta

from dcas.tests.base import DCASPipelineBaseTest
from dcas.functions import (
    calculate_growth_stage, get_last_message_date,
    filter_messages_by_weeks, calculate_message_output
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
        self.assertEqual(row['growth_stage_id'], 13)
        self.assertEqual(row['growth_stage_start_date'], 125)

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
        self.assertEqual(row['growth_stage_start_date'], 123)

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
        self.assertEqual(row['growth_stage_id'], 13)
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
        self.assertEqual(row['growth_stage_id'], 13)
        self.assertEqual(row['growth_stage_start_date'], 125)

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
        self.assertEqual(row['growth_stage_id'], 13)
        self.assertEqual(row['growth_stage_start_date'], 125)

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
            'message': ['1001', '1002', '1001', '1003', '1001', '1004'],
            'message_2': [None, '1001', None, None, '1003', None],
            'message_3': [None, None, '1001', None, None, None],
            'message_4': [None, None, None, None, None, None],
            'message_5': [None, None, None, '1001', None, '1004'],
            'message_date': [
                now - timedelta(days=15),  # 1001 - Oldest farm 1, crop 100
                now - timedelta(days=10),  # 1002
                now - timedelta(days=5),   # 1001 - More recent
                now - timedelta(days=12),  # 1003
                now - timedelta(days=3),   # 1001 - Most recent
                now - timedelta(days=20)   # 1004 - Oldest
            ]
        })

        # Simulate `read_grid_crop_data` returning the dataset
        mock_read_grid_crop_data.return_value = mock_data

        # Pre-filter messages for farm 2
        farm_messages_farm_2 = mock_data[mock_data["farm_id"] == 2]

        # Latest 1001 for farm 2, crop 101 should be at index 4 (3 days ago)
        result = get_last_message_date(farm_messages_farm_2, 101, "1001")
        self.assertEqual(result, mock_data['message_date'].iloc[4])

        # Latest 1003 for farm 2, crop 101 should be at index 3 (12 days ago)
        result = get_last_message_date(farm_messages_farm_2, 101, "1003")
        self.assertEqual(result, mock_data['message_date'].iloc[4])

        # Pre-filter messages for farm 1
        farm_messages_farm_1 = mock_data[mock_data["farm_id"] == 1]

        # Latest 1002 for farm 1, crop 100 should be at index 1 (10 days ago)
        result = get_last_message_date(farm_messages_farm_1, 100, "1002")
        self.assertEqual(result, mock_data['message_date'].iloc[1])

        # Latest 1001 for farm 1, crop 100 should be at index 2 (5 days ago)
        result = get_last_message_date(farm_messages_farm_1, 100, "1001")
        self.assertEqual(result, mock_data['message_date'].iloc[2])

        # MSG5 does not exist in the dataset for farm 2, crop 101
        result = get_last_message_date(farm_messages_farm_2, 101, "1005")
        self.assertIsNone(result)

    @patch("dcas.functions.read_grid_crop_data")
    def test_get_last_message_date_not_exists(self, mock_read_grid_crop_data):
        """Test when the message does not exist in history."""
        now = pd.Timestamp(datetime.now())

        # Mock DataFrame with different messages, but not "1001"
        mock_data = pd.DataFrame({
            'farm_id': [1, 1, 2],
            'crop_id': [100, 100, 101],
            'message': ['1002', '1003', '1004'],
            'message_2': ['1005', None, None],  # Different message
            'message_3': [None, '1006', None],  # Different message
            'message_4': [None, None, '1007'],  # Different message
            'message_5': [None, None, None],  # No relevant messages
            'message_date': [
                now - timedelta(days=10),  # 1002
                now - timedelta(days=5),   # 1003
                now - timedelta(days=3)    # 1004
            ]
        })

        mock_read_grid_crop_data.return_value = mock_data

        # Attempting to get "1001", which is not present in the history
        result = get_last_message_date(mock_data, 100, "1001")

        # Ensure that the function correctly returns None
        self.assertIsNone(result)

    @patch("dcas.functions.read_grid_crop_data")
    def test_get_last_message_date_multiple_messages(
        self, mock_read_grid_crop_data
    ):
        """
        Test when the same message appears multiple times.

        It should return the most recent timestamp.
        """
        # Mock DataFrame representing historical messages
        mock_data = pd.DataFrame({
            'farm_id': [1, 1, 1],
            'crop_id': [100, 100, 100],
            'message': ['1001', '1001', '1001'],
            'message_2': [None, None, None],
            'message_3': [None, None, None],
            'message_4': [None, None, None],
            'message_5': [None, None, None],
            'message_date': [
                pd.Timestamp(datetime.now() - timedelta(days=15)),  # Oldest
                pd.Timestamp(datetime.now() - timedelta(days=7)),   # Middle
                pd.Timestamp(datetime.now() - timedelta(days=2))    # recent
            ]
        })

        # Mock return value for read_grid_crop_data
        mock_read_grid_crop_data.return_value = mock_data

        # Pre-filter data to simulate getting farm messages
        farm_messages = mock_data[mock_data["farm_id"] == 1]

        # Call function with the updated parameters
        result = get_last_message_date(farm_messages, 100, "1001")

        # Expected result: Most recent message date
        expected_result = mock_data['message_date'].max()

        # Assertions
        self.assertEqual(
            result,
            expected_result,
            f"Expected {expected_result}, but got {result}"
        )

    @patch("dcas.functions.read_grid_crop_data")
    def test_filter_messages_by_weeks(self, mock_read_grid_crop_data):
        """Test filtering messages based on the time constraint (weeks)."""
        test_weeks = 2  # Remove messages sent within the last 2 weeks
        current_date = pd.Timestamp(datetime.now())  # Fixed datetime

        # Mock input DataFrame (new messages)
        df = pd.DataFrame({
            'farm_id': [1, 2, 3],
            'crop_id': [100, 200, 300],
            'message': ['1001', '1002', '1003'],
            'message_2': [None, None, None],
            'message_3': [None, None, None],
            'message_4': [None, None, None],
            'message_5': [None, None, None],
        })

        # Mock historical messages (Parquet data)
        historical_df = pd.DataFrame({
            'farm_id': [1, 2],  # Only farms 1 and 2 have historical messages
            'crop_id': [100, 200],
            'message': ['1001', '1002'],
            'message_2': [None, None],
            'message_3': [None, None],
            'message_4': [None, None],
            'message_5': [None, None],
            'message_date': [
                current_date - timedelta(weeks=1),  # Recent
                current_date - timedelta(weeks=3)],  # Older
        })

        # Mock `read_grid_crop_data` to return the historical messages
        mock_read_grid_crop_data.return_value = historical_df

        # Run function
        filtered_df = filter_messages_by_weeks(df, "/fake/path", test_weeks)

        # Assertions
        self.assertIsNone(filtered_df.loc[0, 'message'])
        self.assertEqual(filtered_df.loc[1, 'message'], '1002')
        self.assertEqual(filtered_df.loc[2, 'message'], '1003')

        # Ensure `read_grid_crop_data` was called once
        mock_read_grid_crop_data.assert_called_once_with("/fake/path", [], [])

    @patch('dcas.service.MessagePriorityService.sort_messages')
    def test_calculate_message_output(self, mock_sort_messages):
        """Test calculate_message_output with mocked rule engine."""
        # Input data for the function
        input_data = {
            'farm_id': 1,
            'crop_id': 100,
            'growth_stage_id': 2,
            'crop_stage_type_id': 2,
            'config_id': 1,
            'gdd_sum': 450,
            'temperature': 25,
            'prev_week_message': None,
        }
        attrib_dict = {
            'temperature': 1
        }

        # Mock the rule engine's behavior
        def mock_execute_rule(input_data):
            input_data.message_codes.add('1001')
            input_data.message_codes.add('1002')
        mock_rule_engine = MagicMock()
        mock_rule_engine.execute_rule.side_effect = mock_execute_rule
        mock_sort_messages.return_value = ['1001', '1002']

        # Call the function
        result = calculate_message_output(
            input_data,
            mock_rule_engine,
            attrib_dict
        )

        # Assertions
        self.assertIn('message', result)
        self.assertIn('message_2', result)
        self.assertIn('is_empty_message', result)
        self.assertIn('final_message', result)
        self.assertEqual(result['message'], 1001)
        self.assertEqual(result['message_2'], 1002)
        self.assertEqual(result['is_empty_message'], False)
        self.assertEqual(result['final_message'], 1001)

        # Ensure the rule engine was called with the correct parameters
        mock_rule_engine.execute_rule.assert_called_once()
        mock_sort_messages.assert_called_once()

    def test_calculate_message_output_empty(self):
        """Calculate message output with empty message codes."""
        # Input data for the function
        input_data = {
            'farm_id': 1,
            'crop_id': 100,
            'growth_stage_id': 2,
            'crop_stage_type_id': 2,
            'config_id': 1,
            'gdd_sum': 450,
            'temperature': 25,
            'prev_week_message': None,
        }
        attrib_dict = {
            'temperature': 1
        }

        # Mock the rule engine's behavior
        def mock_execute_rule(input_data):
            pass
        mock_rule_engine = MagicMock()
        mock_rule_engine.execute_rule.side_effect = mock_execute_rule

        # Call the function
        result = calculate_message_output(
            input_data,
            mock_rule_engine,
            attrib_dict
        )

        # Assertions
        self.assertNotIn('message', result)
        self.assertNotIn('message_2', result)
        self.assertIn('is_empty_message', result)
        self.assertNotIn('final_message', result)
        self.assertEqual(result['is_empty_message'], True)

        # Ensure the rule engine was called with the correct parameters
        mock_rule_engine.execute_rule.assert_called_once()

    @patch('dcas.service.MessagePriorityService.sort_messages')
    def test_calculate_message_output_repetitive(self, mock_sort_messages):
        """Test calculate_message_output with repetitive messages."""
        # Input data for the function
        input_data = {
            'farm_id': 1,
            'crop_id': 100,
            'growth_stage_id': 2,
            'crop_stage_type_id': 2,
            'config_id': 1,
            'gdd_sum': 450,
            'temperature': 25,
            'prev_week_message': 1001,
        }
        attrib_dict = {
            'temperature': 1
        }

        # Mock the rule engine's behavior
        def mock_execute_rule(input_data):
            input_data.message_codes.add('1001')
            input_data.message_codes.add('1002')
        mock_rule_engine = MagicMock()
        mock_rule_engine.execute_rule.side_effect = mock_execute_rule
        mock_sort_messages.return_value = ['1001', '1002']

        # Call the function
        result = calculate_message_output(
            input_data,
            mock_rule_engine,
            attrib_dict
        )

        # Assertions
        self.assertIn('message', result)
        self.assertIn('message_2', result)
        self.assertIn('is_empty_message', result)
        self.assertIn('has_repetitive_message', result)
        self.assertIn('final_message', result)
        self.assertEqual(result['message'], 1001)
        self.assertEqual(result['message_2'], 1002)
        self.assertEqual(result['is_empty_message'], False)
        self.assertEqual(result['has_repetitive_message'], True)
        self.assertEqual(result['final_message'], 1002)

        # Ensure the rule engine was called with the correct parameters
        mock_rule_engine.execute_rule.assert_called_once()
        mock_sort_messages.assert_called_once()
