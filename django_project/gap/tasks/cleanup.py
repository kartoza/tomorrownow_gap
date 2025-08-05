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
from django.db import connection

from core.models.background_task import TaskStatus
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
        return count, 0

    logger.info(f"Deleting {count} Zarr files marked as deleted")
    deleted_ids = []
    failed_deletions = []
    for ds_file in qs:
        if not ds_file.should_delete():
            logger.info(
                f"Skipping {ds_file.name} as it is not ready for deletion"
            )
            continue

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

        # check if success
        deletion_log.refresh_from_db()
        if deletion_log.status != TaskStatus.COMPLETED:
            logger.error(
                f"Failed to delete {ds_file.name} with path: {path}. "
                f"Status: {deletion_log.status}"
            )
            failed_deletions.append(ds_file.id)
        else:
            deleted_ids.append(ds_file.id)

    # Delete the files
    if deleted_ids:
        DataSourceFile.objects.filter(id__in=deleted_ids).delete()
        logger.info(
            f"{len(deleted_ids)} DataSourceFiles have been deleted..."
        )
    if failed_deletions:
        logger.error(
            f"Failed to delete {len(failed_deletions)} DataSourceFiles: "
            f"{failed_deletions}"
        )
    return len(deleted_ids), len(failed_deletions)


@app.task(name='cleanup_old_forecast_data')
def cleanup_old_forecast_data(cutoff_days: int = 14):
    """Delete old forecast data older than 14 days."""
    cutoff = timezone.now() - timedelta(days=cutoff_days)
    logger.info(
        f"Deleting FarmShortTermForecastData older than {cutoff}"
    )
    with connection.cursor() as cursor:
        cursor.execute("""
            DELETE FROM gap_farmshorttermforecastdata gf
            WHERE gf.forecast_id in (
                SELECT id
                FROM gap_farmshorttermforecast
                WHERE forecast_date < %s
            )
        """, [cutoff])
        cursor.execute("""
            DELETE FROM gap_farmshorttermforecast
            WHERE forecast_date < %s
        """, [cutoff])
