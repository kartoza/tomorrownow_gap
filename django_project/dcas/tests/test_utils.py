# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS utilities functions.
"""

from mock import patch, MagicMock

from dcas.tests.base import DCASPipelineBaseTest
from dcas.utils import (
    read_grid_crop_data, read_grid_data, get_previous_week_message
)


class DCASUtilsTest(DCASPipelineBaseTest):
    """DCAS utilities test case."""

    @patch('dcas.outputs.duckdb.connect')
    def test_read_grid_data(self, mock_duckdb_connect):
        """Test read_grid_data function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        read_grid_data('test.parquet', ['column_1'], [1])

        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('dcas.outputs.duckdb.connect')
    def test_read_grid_data_with_threads(self, mock_duckdb_connect):
        """Test read_grid_data function with threads."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        read_grid_data('test.parquet', ['column_1'], [1], 2)

        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('dcas.outputs.duckdb.connect')
    def test_read_grid_crop_data(self, mock_duckdb_connect):
        """Test read_grid_crop_data function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        read_grid_crop_data('test.parquet', ['1_1_1'], None)

        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()


    @patch('dcas.outputs.duckdb.connect')
    def test_read_grid_crop_data_with_threads(self, mock_duckdb_connect):
        """Test read_grid_crop_data function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        read_grid_crop_data('test.parquet', ['1_1_1'], 2)

        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('dcas.outputs.duckdb.connect')
    def test_get_previous_week_message(self, mock_duckdb_connect):
        """Test get_previous_week_message function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        get_previous_week_message('test.parquet', ['column_1'])

        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()
