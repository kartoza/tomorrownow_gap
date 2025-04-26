# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS utilities functions.
"""

from mock import patch, MagicMock

from dcas.tests.base import DCASPipelineBaseTest
from dcas.utils import (
    read_grid_crop_data, read_grid_data,
    remove_dcas_output_file,
    dcas_output_file_exists,
    get_previous_week_message
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

    @patch('dcas.utils.storages')
    def test_remove_dcas_output_file_success(self, mock_storages):
        """Test remove_dcas_output_file function for successful deletion."""
        # Mock the storage object
        mock_s3_storage = MagicMock()
        mock_storages.__getitem__.return_value = mock_s3_storage

        # Call the function
        result = remove_dcas_output_file('test_file.txt', 'OBJECT_STORAGE')

        # Assertions
        mock_storages.__getitem__.assert_called_once_with('gap_products')
        mock_s3_storage.delete.assert_called_once_with('test_file.txt')
        self.assertTrue(result)

    @patch('dcas.utils.storages')
    def test_remove_dcas_output_file_failure(self, mock_storages):
        """Test remove_dcas_output_file function for deletion failure."""
        # Mock the storage object
        mock_s3_storage = MagicMock()
        mock_storages.__getitem__.return_value = mock_s3_storage

        # Simulate an exception during deletion
        mock_s3_storage.delete.side_effect = Exception('Deletion error')

        # Call the function
        result = remove_dcas_output_file('test_file.txt', 'OBJECT_STORAGE')

        # Assertions
        mock_storages.__getitem__.assert_called_once_with('gap_products')
        mock_s3_storage.delete.assert_called_once_with('test_file.txt')
        self.assertFalse(result)

    def test_remove_dcas_output_file_not_implemented(self):
        """Test remove_dcas_output_file function for unsupported delivery."""
        with self.assertRaises(NotImplementedError) as context:
            remove_dcas_output_file('test_file.txt', 'LOCAL_STORAGE')

        self.assertEqual(
            str(context.exception),
            "This function is not implemented for LOCAL_STORAGE delivery."
        )

    @patch('dcas.utils.storages')
    def test_dcas_output_file_exists_true(self, mock_storages):
        """Test dcas_output_file_exists function when file exists."""
        # Mock the storage object
        mock_s3_storage = MagicMock()
        mock_storages.__getitem__.return_value = mock_s3_storage

        # Simulate file existence
        mock_s3_storage.exists.return_value = True

        # Call the function
        result = dcas_output_file_exists('test_file.txt', 'OBJECT_STORAGE')

        # Assertions
        mock_storages.__getitem__.assert_called_once_with('gap_products')
        mock_s3_storage.exists.assert_called_once_with('test_file.txt')
        self.assertTrue(result)

    @patch('dcas.utils.storages')
    def test_dcas_output_file_exists_false(self, mock_storages):
        """Test dcas_output_file_exists function when file does not exist."""
        # Mock the storage object
        mock_s3_storage = MagicMock()
        mock_storages.__getitem__.return_value = mock_s3_storage

        # Simulate file non-existence
        mock_s3_storage.exists.return_value = False

        # Call the function
        result = dcas_output_file_exists('test_file.txt', 'OBJECT_STORAGE')

        # Assertions
        mock_storages.__getitem__.assert_called_once_with('gap_products')
        mock_s3_storage.exists.assert_called_once_with('test_file.txt')
        self.assertFalse(result)

    def test_dcas_output_file_exists_not_implemented(self):
        """Test dcas_output_file_exists function for unsupported delivery."""
        with self.assertRaises(NotImplementedError) as context:
            dcas_output_file_exists('test_file.txt', 'LOCAL_STORAGE')

        self.assertEqual(
            str(context.exception),
            "This function is not implemented for LOCAL_STORAGE delivery."
        )
