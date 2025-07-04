# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Cleanup Tasks.
"""

import os
from celery.utils.log import get_task_logger
from core.celery import app
from django.utils import timezone
from datetime import timedelta

from core.models.object_storage_manager import (
    ObjectStorageManager,
    DeletionLog
)
from gap.models.signup_request import SignUpRequest, RequestStatus
from gap.models.dataset import DatasetStore, DataSourceFile

logger = get_task_logger(__name__)


@app.task(name='cleanup_incomplete_signups')
def cleanup_incomplete_signups():
    """Delete any SignUpRequest still INCOMPLETE older than 30 days."""
    cutoff = timezone.now() - timedelta(days=30)
    qs = SignUpRequest.objects.filter(
        status=RequestStatus.INCOMPLETE,
        submitted_at__lt=cutoff
    )
    count = qs.count()
    qs.delete()
    return f"Deleted {count} old incomplete SignUpRequests"


@app.task(name='cleanup_deleted_zarr')
def cleanup_deleted_zarr():
    """Remove Zarr files marked as deleted."""
    qs = DataSourceFile.objects.filter(
        format=DatasetStore.ZARR,
        deleted_at__isnull=False,
        is_latest=False
    )
    count = qs.count()
    if count == 0:
        return count

    logger.info(f"Deleting {count} Zarr files marked as deleted")
    for ds_file in qs:
        logger.info(f"Deleting Zarr file: {ds_file.name}")
        connection_name = ds_file.metadata.get(
            'connection_name', 'default'
        )
        connection = ObjectStorageManager.objects.filter(
            connection_name=connection_name
        ).first()
        if connection is None:
            logger.warning(
                f"Connection {connection_name} not found "
                f"for file {ds_file.name}"
            )
            continue

        # Get the S3 variables
        s3_env_vars = connection.get_s3_env_vars(
            connection_name=connection_name
        )
        path = ds_file.name
        if not path.startswith(s3_env_vars['S3_DIR_PREFIX']):
            path = os.path.join(s3_env_vars['S3_DIR_PREFIX'], path)
        if not path.endswith('/'):
            path += '/'

        # Log the deletion
        deletion_log = DeletionLog.objects.create(
            object_storage_manager=connection,
            path=path,
            is_directory=True,
            deleted_at=timezone.now()
        )

        logger.info(f"Deleting {ds_file.name} with path: {path}")
        deletion_log.run()

    # Delete the files
    qs.delete()
    return count
