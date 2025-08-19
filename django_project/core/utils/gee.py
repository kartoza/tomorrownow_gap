# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for Google Earth Engine.
"""

import os
import base64
import ee
import logging

logger = logging.getLogger(__name__)
TASK_WAIT_TIME = 5  # seconds


def initialize_earth_engine():
    """Initialize the Earth Engine API for analysis."""
    service_account_key = os.environ.get('GCS_SERVICE_ACCOUNT_KEY', '')
    service_account = os.environ.get('GCS_SERVICE_ACCOUNT', '')
    if os.path.exists(service_account_key):
        credentials = ee.ServiceAccountCredentials(
            service_account,
            service_account_key
        )
    else:
        credentials = ee.ServiceAccountCredentials(
            service_account,
            key_data=base64.b64decode(service_account_key).decode('utf-8')
        )
    try:
        # Initialize the Earth Engine API with the service account
        ee.Initialize(credentials)
        logger.info("Earth Engine initialized successfully.")
    except ee.EEException as e:
        logger.error(
            "Earth Engine initialization failed: %s", e, exc_info=True
        )
