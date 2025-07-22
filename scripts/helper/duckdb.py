# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for DuckDB.
"""

import duckdb

from config import get_settings


def get_duckdb_connection():
    """Get a DuckDB connection."""
    settings = get_settings()
    endpoint = settings.S3_ENDPOINT_URL
    # Remove protocol from endpoint
    endpoint = endpoint.replace('http://', '')
    endpoint = endpoint.replace('https://', '')
    if endpoint.endswith('/'):
        endpoint = endpoint[:-1]

    config = {
        's3_access_key_id': settings.S3_ACCESS_KEY_ID,
        's3_secret_access_key': settings.S3_SECRET_ACCESS_KEY,
        's3_region': 'us-east-1',
        's3_url_style': 'path',
        's3_endpoint': endpoint,
        's3_use_ssl': settings.DUCKDB_USE_SSL,
        'memory_limit': settings.DUCKDB_MEMORY_LIMIT,
        'threads': settings.DUCKDB_NUM_THREADS
    }

    conn = duckdb.connect(config=config)
    conn.install_extension('httpfs')
    conn.load_extension('httpfs')
    conn.install_extension('spatial')
    conn.load_extension('spatial')

    return conn
