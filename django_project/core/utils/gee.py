# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for Google Earth Engine.
"""

import os
import time
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


def start_export_task(task: ee.batch.Task, description, is_async=False):
    """Start an export task and logs its status.

    Args:
        task (ee.batch.Task): The Earth Engine export task to start.
        description (str): A description of the export task.
        is_async (bool, optional): Whether to return
            the task status asynchronously. Defaults to False.

    Returns:
        dict: The status of the export task.
    """
    task.start()
    logger.info(f"Export task '{description}' started.")

    status = task.status()
    if is_async:
        return status

    while task.active():
        status = task.status()
        logger.info(f"Task status: {status['state']}")
        time.sleep(TASK_WAIT_TIME)

    final_status = task.status()
    logger.info(f"Final task status: {final_status['state']}")
    if final_status['state'] == 'COMPLETED':
        logger.info('Export completed successfully.')
    else:
        logger.info('Export failed. Details:')
        logger.info(final_status)
    return final_status
