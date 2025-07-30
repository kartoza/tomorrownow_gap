# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for S3 utils.
"""

from mock import patch, Mock
import requests
from django.test import TestCase

from core.utils.url_file_checker import file_exists_at_url


class TestFileExistsAtUrl(TestCase):
    """Test the file_exists_at_url utility function."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.valid_http_url = "https://example.com/file.txt"
        self.valid_https_url = "https://secure.example.com/document.pdf"
        self.valid_file_url = "file:///home/user/document.txt"
        self.valid_windows_file_url = "file:///C:/Users/user/document.txt"
        self.local_path = "/home/user/document.txt"
        self.windows_local_path = "C:\\Users\\user\\document.txt"

    @patch('requests.head')
    def test_http_url_exists(self, mock_head):
        """Test HTTP URL that exists (returns 200)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        result = file_exists_at_url(self.valid_http_url)

        self.assertTrue(result)
        mock_head.assert_called_once_with(
            self.valid_http_url, timeout=10, allow_redirects=True
        )

    @patch('requests.head')
    def test_https_url_exists(self, mock_head):
        """Test HTTPS URL that exists (returns 200)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        result = file_exists_at_url(self.valid_https_url)

        self.assertTrue(result)
        mock_head.assert_called_once_with(
            self.valid_https_url, timeout=10, allow_redirects=True
        )

    @patch('requests.head')
    def test_http_url_not_found(self, mock_head):
        """Test HTTP URL that doesn't exist (returns 404)."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_head.return_value = mock_response

        result = file_exists_at_url(self.valid_http_url)

        self.assertFalse(result)
        mock_head.assert_called_once_with(
            self.valid_http_url, timeout=10, allow_redirects=True
        )

    @patch('requests.head')
    def test_http_url_server_error(self, mock_head):
        """Test HTTP URL that returns server error (500)."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_head.return_value = mock_response

        result = file_exists_at_url(self.valid_http_url)

        self.assertFalse(result)

    @patch('requests.head')
    def test_http_url_network_error(self, mock_head):
        """Test HTTP URL with network error."""
        mock_head.side_effect = requests.exceptions.ConnectionError(
            "Network error"
        )

        result = file_exists_at_url(self.valid_http_url)

        self.assertFalse(result)

    @patch('requests.head')
    def test_http_url_timeout(self, mock_head):
        """Test HTTP URL with timeout."""
        mock_head.side_effect = requests.exceptions.Timeout("Request timeout")

        result = file_exists_at_url(self.valid_http_url)

        self.assertFalse(result)

    @patch('requests.head')
    def test_custom_timeout(self, mock_head):
        """Test HTTP URL with custom timeout."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        result = file_exists_at_url(self.valid_http_url, timeout=30)

        self.assertTrue(result)
        mock_head.assert_called_once_with(
            self.valid_http_url, timeout=30, allow_redirects=True
        )

    @patch('os.path.exists')
    def test_file_url_exists_unix(self, mock_exists):
        """Test file:// URL that exists on Unix system."""
        mock_exists.return_value = True

        result = file_exists_at_url(self.valid_file_url)

        self.assertTrue(result)
        mock_exists.assert_called_once_with('/home/user/document.txt')

    @patch('os.path.exists')
    def test_file_url_not_exists_unix(self, mock_exists):
        """Test file:// URL that doesn't exist on Unix system."""
        mock_exists.return_value = False

        result = file_exists_at_url(self.valid_file_url)

        self.assertFalse(result)
        mock_exists.assert_called_once_with('/home/user/document.txt')

    @patch('os.path.exists')
    def test_local_path_exists(self, mock_exists):
        """Test local path without scheme that exists."""
        mock_exists.return_value = True

        result = file_exists_at_url(self.local_path)

        self.assertTrue(result)
        mock_exists.assert_called_once_with(self.local_path)

    @patch('os.path.exists')
    def test_local_path_not_exists(self, mock_exists):
        """Test local path without scheme that doesn't exist."""
        mock_exists.return_value = False

        result = file_exists_at_url(self.local_path)

        self.assertFalse(result)
        mock_exists.assert_called_once_with(self.local_path)

    @patch('os.path.exists')
    def test_local_path_file_system_error(self, mock_exists):
        """Test local path with file system error."""
        mock_exists.side_effect = OSError("Permission denied")

        result = file_exists_at_url(self.local_path)

        self.assertFalse(result)

    def test_unsupported_scheme(self):
        """Test URL with unsupported scheme."""
        unsupported_url = "ftp://example.com/file.txt"

        with self.assertRaises(ValueError) as context:
            file_exists_at_url(unsupported_url)

        self.assertIn("Unsupported URL scheme: ftp", str(context.exception))

    def test_malformed_url(self):
        """Test malformed URL."""
        malformed_url = "not_a_valid_url"

        # This should be treated as a local path
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            result = file_exists_at_url(malformed_url)
            self.assertFalse(result)
            mock_exists.assert_called_once_with(malformed_url)

    def test_empty_url(self):
        """Test empty URL."""
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            result = file_exists_at_url("")
            self.assertFalse(result)
            mock_exists.assert_called_once_with("")

    @patch('requests.head')
    def test_http_url_with_redirects(self, mock_head):
        """Test HTTP URL that exists after redirects."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        result = file_exists_at_url("http://example.com/redirect")

        self.assertTrue(result)
        # Verify allow_redirects=True is passed
        mock_head.assert_called_once_with(
            "http://example.com/redirect", timeout=10, allow_redirects=True
        )

    def test_case_insensitive_schemes(self):
        """Test that URL schemes are handled case-insensitively."""
        with patch('requests.head') as mock_head:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_head.return_value = mock_response

            # Test uppercase HTTP
            result = file_exists_at_url("HTTP://example.com/file.txt")
            self.assertTrue(result)

            # Test mixed case HTTPS
            result = file_exists_at_url("HttpS://example.com/file.txt")
            self.assertTrue(result)

        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            # Test uppercase FILE
            result = file_exists_at_url("FILE:///home/user/file.txt")
            self.assertTrue(result)
