# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for File Checker.
"""

import os
import requests
from urllib.parse import urlparse


def file_exists_at_url(url: str, timeout: int = 10) -> bool:
    """
    Check if a file exists at the given URL.
    
    Args:
        url (str): The URL to check (supports HTTP/HTTPS and file:// schemes)
        timeout (int): Timeout in seconds for HTTP requests (default: 10)
    
    Returns:
        bool: True if the file exists, False otherwise
    
    Raises:
        ValueError: If the URL scheme is not supported
    """
    try:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        print(f'scheme: {scheme}, url: {url}')  # Debugging output
        if scheme in ['http', 'https']:
            # For remote URLs, use HEAD request to check existence
            response = requests.head(
                url,
                timeout=timeout,
                allow_redirects=True
            )
            return response.status_code == 200
        elif scheme == 'file':
            # For local file URLs, extract path and check with os.path.exists
            file_path = parsed_url.path
            return os.path.exists(file_path)
        elif scheme == '':
            # If no scheme provided, assume it's a local file path
            return os.path.exists(url)
        else:
            raise ValueError(f"Unsupported URL scheme: {scheme}")
    except requests.exceptions.RequestException:
        # Network errors, timeouts, etc.
        return False
    except OSError:
        # Local file errors (e.g., permission denied)
        return False
