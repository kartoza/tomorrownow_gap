# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Outputs functions.
"""

import csv
import os
import datetime
from mock import patch, MagicMock

from dcas.tests.base import DCASPipelineBaseTest
from dcas.outputs import DCASPipelineOutput


class DCASOutputsTest(DCASPipelineBaseTest):
    """DCAS Outputs test case."""

    @patch('dcas.outputs.duckdb.connect')
    def test_convert_to_csv(self, mock_duckdb_connect):
        """Test convert_to_csv function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        data_output = DCASPipelineOutput(datetime.date(2025, 1, 1))
        data_output._setup_s3fs()
        os.makedirs(data_output.TMP_BASE_DIR, exist_ok=True)
        with open(
            os.path.join(data_output.TMP_BASE_DIR, 'output_20250101.csv'),
            'w',
            newline=''
        ) as file:
            writer = csv.writer(file)
            field = ["farmerId", "crop", "plantingDate"]
            writer.writerow(field)

        csv_file = data_output.convert_to_csv()

        mock_duckdb_connect.assert_called_once()
        mock_conn.install_extension.assert_any_call("httpfs")
        mock_conn.load_extension.assert_any_call("httpfs")
        mock_conn.install_extension.assert_any_call("spatial")
        mock_conn.load_extension.assert_any_call("spatial")
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()
        self.assertIn('output_20250101.csv', csv_file)

    @patch("paramiko.SFTPClient.from_transport")
    @patch("paramiko.Transport", autospec=True)
    @patch("dcas.outputs.settings")
    def test_upload_to_sftp(
        self,
        mock_settings,
        mock_transport,
        mock_sftp_from_transport
    ):
        """Test that SFTP upload is triggered correctly."""
        # Set mock environment variables in `settings`
        mock_settings.SFTP_HOST = "127.0.0.1"
        mock_settings.SFTP_PORT = 2222
        mock_settings.SFTP_USERNAME = "user"
        mock_settings.SFTP_PASSWORD = "password"
        mock_settings.SFTP_REMOTE_PATH = "upload"

        # Mock the transport instance
        mock_transport_instance = mock_transport.return_value
        mock_transport_instance.connect.return_value = None

        # Mock the SFTP client and `put()` method
        mock_sftp_instance = MagicMock()
        mock_sftp_from_transport.return_value = mock_sftp_instance
        mock_sftp_instance.put.return_value = None

        # Initialize the pipeline output
        pipeline_output = DCASPipelineOutput(request_date="2025-01-15")

        # Create a temporary file for testing
        test_file = "/tmp/test_message_data.csv"
        with open(test_file, "w") as f:
            f.write("test message")

        # Call the method under test
        pipeline_output.upload_to_sftp(test_file)

        # Assertions
        # Verify `connect()` was called once
        mock_transport_instance.connect.assert_called_once_with(
            username=mock_settings.SFTP_USERNAME,
            password=mock_settings.SFTP_PASSWORD
        )

        # Verify `SFTPClient.from_transport()` was called once
        mock_sftp_from_transport.assert_called_once_with(
            mock_transport_instance
        )

        # Verify file was uploaded via `put()`
        mock_sftp_instance.put.assert_called_once_with(
            test_file,
            f'{mock_settings.SFTP_REMOTE_PATH}/test_message_data.csv'
        )

        # Verify SFTP client & transport were properly closed
        mock_sftp_instance.close.assert_called_once()
        mock_transport_instance.close.assert_called_once()
