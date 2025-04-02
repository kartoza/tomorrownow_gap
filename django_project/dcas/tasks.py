# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Tasks
"""

import os
from celery import shared_task
import traceback
import datetime
import logging
import tempfile
from django.core.files.storage import storages
from django.utils import timezone

from core.models.background_task import TaskStatus
from gap.models import FarmRegistryGroup, FarmRegistry, Preferences, Farm
from dcas.models import (
    DCASErrorLog, DCASRequest, DCASErrorType,
    DCASOutput, DCASDeliveryMethod
)
from dcas.pipeline import DCASDataPipeline
from dcas.queries import DataQuery
from dcas.outputs import DCASPipelineOutput


logger = logging.getLogger(__name__)
DCAS_OBJECT_STORAGE_DIR = 'dcas_csv'


class DCASPreferences:
    """Class that manages the configuration of DCAS process."""

    def __init__(self, current_date: datetime.date):
        """Initialize DCASPreferences class."""
        self.dcas_config = Preferences.load().dcas_config
        self.current_date = current_date

    @property
    def request_date(self):
        """Get the request date for pipeline."""
        override_dt = self.dcas_config.get('override_request_date', None)
        if override_dt:
            return datetime.date.fromisoformat(override_dt)
        return self.current_date

    @property
    def is_scheduled_to_run(self):
        """Check if pipeline should be run based on config."""
        return self.request_date.weekday() in self.dcas_config.get('weekdays')

    @property
    def farm_registry_groups(self):
        """Get farm registry group ids that will be used in the pipeline."""
        group_ids = self.dcas_config.get('farm_registries', [])
        if len(group_ids) == 0:
            # use the latest
            farm_registry_group = FarmRegistryGroup.objects.filter(
                is_latest=True
            ).first()
            if farm_registry_group:
                group_ids.append(farm_registry_group.id)
        return group_ids

    @property
    def farm_num_partitions(self):
        """Get the number of partitions for farms dataframe."""
        # TODO: we could calculate the number of partitions
        # based on total count
        return self.dcas_config.get('farm_npartitions', None)

    @property
    def grid_crop_num_partitions(self):
        """Get the number of partitions for grid and crop dataframe."""
        # TODO: we could calculate the number of partitions
        # based on total count
        return self.dcas_config.get('grid_crop_npartitions', None)

    @property
    def duck_db_num_threads(self):
        """Get the number of threads for duckdb."""
        return Preferences.load().duckdb_threads_num

    @property
    def store_csv_to_minio(self):
        """Check if process should store csv to minio."""
        return self.dcas_config.get('store_csv_to_minio', False)

    @property
    def store_csv_to_sftp(self):
        """Check if process should store csv to sftp."""
        return self.dcas_config.get('store_csv_to_sftp', False)

    @property
    def trigger_error_handling(self):
        """Check if process should trigger error handling."""
        return self.dcas_config.get('trigger_error_handling', False)

    def to_dict(self):
        """Export the config to dict."""
        return {
            'request_date': self.request_date.isoformat(),
            'weekdays': self.dcas_config.get('weekdays'),
            'is_scheduled_to_run': self.is_scheduled_to_run,
            'farm_registry_groups': self.farm_registry_groups,
            'farm_num_partitions': self.farm_num_partitions,
            'grid_crop_num_partitions': self.grid_crop_num_partitions,
            'duck_db_num_threads': self.duck_db_num_threads,
            'store_csv_to_minio': self.store_csv_to_minio,
            'store_csv_to_sftp': self.store_csv_to_sftp,
            'trigger_error_handling': self.trigger_error_handling
        }

    @staticmethod
    def object_storage_path(filename):
        """Return object storage upload path for csv output."""
        dir_prefix = os.environ.get('MINIO_GAP_AWS_DIR_PREFIX', '')
        if dir_prefix and not dir_prefix.endswith('/'):
            dir_prefix += '/'
        return (
            f'{dir_prefix}{DCAS_OBJECT_STORAGE_DIR}/'
            f'{filename}'
        )


def save_dcas_ouput_to_object_storage(file_path):
    """Store dcas csv file to object_storage."""
    s3_storage = storages['gap_products']
    with open(file_path) as content:
        s3_storage.save(
            DCASPreferences.object_storage_path(
                os.path.basename(file_path)
            ),
            content
        )
    return True


def export_dcas_output(request_id, delivery_method):
    """Export dcas output csv by delivery_method."""
    dcas_request = DCASRequest.objects.get(
        id=request_id
    )
    dcas_output = DCASPipelineOutput(
        dcas_request.requested_at.date(),
        duck_db_num_threads=Preferences.load().duckdb_threads_num
    )

    dcas_ouput_file = DCASOutput.objects.create(
        request=dcas_request,
        file_name=os.path.basename(dcas_output.output_csv_file_path),
        delivered_at=timezone.now(),
        status=TaskStatus.RUNNING,
        delivery_by=delivery_method
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        dcas_output.TMP_BASE_DIR = tmpdir
        dcas_output._setup_s3fs()
        is_success = False
        file_path = None
        filename = os.path.basename(dcas_output.output_csv_file_path)
        try:
            file_path = dcas_output.convert_to_csv()
            filename = os.path.basename(file_path)

            # save to object storage
            if delivery_method == DCASDeliveryMethod.OBJECT_STORAGE:
                is_success = save_dcas_ouput_to_object_storage(file_path)
            elif delivery_method == DCASDeliveryMethod.SFTP:
                is_success = dcas_output.upload_to_sftp(file_path)
        except Exception as ex:
            logger.error(f'Failed to save dcas output to object storage {ex}')
            raise ex
        finally:
            # store to DCASOutput
            if is_success:
                file_stats = os.stat(file_path)
                dcas_ouput_file.file_name = filename
                dcas_ouput_file.delivered_at = timezone.now()
                dcas_ouput_file.status = TaskStatus.COMPLETED
                dcas_ouput_file.path = (
                    DCASPreferences.object_storage_path(filename)
                )
                dcas_ouput_file.size = file_stats.st_size
            else:
                dcas_ouput_file.status = TaskStatus.STOPPED
            dcas_ouput_file.save()


@shared_task(name="export_dcas_minio")
def export_dcas_minio(request_id):
    """Export DCAS csv output to minio."""
    export_dcas_output(request_id, DCASDeliveryMethod.OBJECT_STORAGE)


@shared_task(name="export_dcas_sftp")
def export_dcas_sftp(request_id):
    """Export DCAS csv output to sftp."""
    export_dcas_output(request_id, DCASDeliveryMethod.SFTP)


@shared_task(name="run_dcas")
def run_dcas(request_id=None):
    """Task to run dcas pipeline."""
    current_dt = timezone.now()

    # create the request object
    dcas_request = None
    if request_id:
        dcas_request = DCASRequest.objects.filter(
            id=request_id
        ).first()

    if dcas_request is None:
        dcas_request = DCASRequest.objects.create(
            requested_at=current_dt,
            status=TaskStatus.PENDING
        )
    else:
        dcas_request.start_time = None
        dcas_request.end_time = None
        dcas_request.status = TaskStatus.PENDING
        dcas_request.progress_text = None
        dcas_request.save()

    dcas_config = DCASPreferences(dcas_request.requested_at.date())

    if not dcas_config.is_scheduled_to_run:
        dcas_request.progress_text = (
            f'DCAS: skipping weekday {dcas_config.request_date.weekday()}'
        )
        dcas_request.save()
        logger.info(dcas_request.progress_text)
        return

    # load farm registry group
    farm_registry_groups = dcas_config.farm_registry_groups
    if len(farm_registry_groups) == 0:
        dcas_request.progress_text = 'DCAS: No farm registry group!'
        dcas_request.save()
        logger.warning(dcas_request)
        return

    # check total count
    total_count = FarmRegistry.objects.filter(
        group_id__in=farm_registry_groups
    ).count()
    logger.info(f'Processing DCAS farm registry: {total_count} records.')
    if total_count == 0:
        dcas_request.progress_text = (
            'DCAS: No farm registry in the registry groups'
        )
        dcas_request.save()
        logger.warning(dcas_request)
        return

    dcas_request.start_time = current_dt
    dcas_request.progress_text = 'Processing farm registry has started!'
    dcas_request.status = TaskStatus.RUNNING
    dcas_request.config = dcas_config.to_dict()
    dcas_request.save()

    # run pipeline
    pipeline = DCASDataPipeline(
        dcas_config.farm_registry_groups, dcas_config.request_date,
        farm_num_partitions=dcas_config.farm_num_partitions,
        grid_crop_num_partitions=dcas_config.grid_crop_num_partitions,
        duck_db_num_threads=dcas_config.duck_db_num_threads
    )

    errors = None
    try:
        logger.info(
            f'Processing DCAS for request_date: {dcas_config.request_date}'
        )
        pipeline.run()
    except Exception as ex:
        errors = str(ex)
        logger.error('Farm registry has errors: ', ex)
        logger.error(traceback.format_exc())
        raise ex
    finally:
        dcas_request.end_time = timezone.now()
        if errors:
            dcas_request.progress_text = (
                f'Processing farm registry has finished with errors: {errors}'
            )
            dcas_request.status = TaskStatus.STOPPED
        else:
            dcas_request.progress_text = (
                'Processing farm registry has finished!'
            )
            dcas_request.status = TaskStatus.COMPLETED
        dcas_request.save()

        if dcas_request.status == TaskStatus.COMPLETED:
            if dcas_config.store_csv_to_minio:
                export_dcas_minio.delay(dcas_request.id)
            elif dcas_config.store_csv_to_sftp:
                export_dcas_sftp.delay(dcas_request.id)

            # Trigger error handling task
            if dcas_config.trigger_error_handling:
                log_farms_without_messages.delay(dcas_request.id)

        # cleanup
        pipeline.cleanup()


@shared_task(name='log_farms_without_messages')
def log_farms_without_messages(request_id, chunk_size=1000):
    """
    Celery task to log farms without messages using chunked queries.

    :param request_id: Id for the pipeline output
    :type request_id: int
    :param chunk_size: Number of rows to process per iteration
    :type chunk_size: int
    """
    try:
        preferences = Preferences.load()
        # Get the most recent DCAS request
        dcas_request = DCASRequest.objects.get(
            id=request_id
        )

        # Initialize pipeline output to get the directory path
        dcas_output = DCASPipelineOutput(
            dcas_request.requested_at.date(),
            duck_db_num_threads=preferences.duckdb_threads_num
        )
        dcas_output._setup_s3fs()
        parquet_path = dcas_output._get_directory_path(
            dcas_output.DCAS_OUTPUT_DIR
        ) + '/iso_a3=*/year=*/month=*/day=*/*.parquet'

        # Clear existing DCASErrorLog
        DCASErrorLog.objects.filter(
            request=dcas_request,
            error_type=DCASErrorType.MISSING_MESSAGES
        ).delete()

        # Query farms without messages in chunks
        for df_chunk in DataQuery.get_farms_without_messages(
            dcas_request.requested_at.date(),
            parquet_path,
            dcas_output._get_connection(dcas_output.s3),
            chunk_size=chunk_size
        ):
            if df_chunk.empty:
                logger.info(
                    "No farms found with missing messages in this chunk."
                )
                continue

            # Log missing messages in the database
            error_logs = []
            for _, row in df_chunk.iterrows():
                if not Farm.objects.filter(id=row['farm_id']).exists():
                    logger.warning(
                        f"Farm ID {row['farm_id']} not found, skipping."
                    )
                    continue

                error_logs.append(DCASErrorLog(
                    request=dcas_request,
                    farm_id=row['farm_id'],
                    error_type=DCASErrorType.MISSING_MESSAGES,
                    error_message=(
                        f"Farm {row['farm_unique_id']} "
                        f"({row['crop']} - {row['growth_stage']}) "
                        f"has no advisory messages."
                    )
                ))

            # Bulk insert logs per chunk to optimize database writes
            if error_logs:
                DCASErrorLog.objects.bulk_create(error_logs)
                logger.info(
                    f"Logged {len(error_logs)} farms with missing messages."
                )

    except DCASRequest.DoesNotExist:
        logger.error(f"No DCASRequest found for request_id {request_id}.")
    except Exception as e:
        logger.error(f"Error processing missing messages: {str(e)}")
