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
from django.db import connection
from django.core.files.storage import storages
from django.utils import timezone
from django.core.mail import send_mail
from django.conf import settings

from core.models.background_task import TaskStatus
from core.utils.emails import get_admin_emails
from gap.models import FarmRegistryGroup, FarmRegistry, Preferences
from dcas.models import (
    DCASErrorLog, DCASRequest, DCASOutput, DCASDeliveryMethod
)
from dcas.pipeline import DCASDataPipeline
from dcas.outputs import DCASPipelineOutput
from dcas.utils import remove_dcas_output_file


logger = logging.getLogger(__name__)
DCAS_OBJECT_STORAGE_DIR = 'dcas_csv'


class DCASPreferences:
    """Class that manages the configuration of DCAS process."""

    default_csv_columns = [
        'farmer_id',
        'message_final',
        'message_english',
        'message_code',
        'crop',
        'planting_date',
        'growth_stage',
        'county',
        'subcounty',
        'ward',
        'relative_humidity',
        'seasonal_precipitation',
        'temperature',
        'ppet',
        'growth_stage_precipitation',
        'growth_stage_date',
        'final_longitude',
        'final_latitude',
        'grid_id'
    ]

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
        threads = self.dcas_config.get('duckdb_threads_num', None)
        if threads:
            return threads
        return Preferences.load().duckdb_threads_num

    @property
    def duck_db_memory_limit(self):
        """Get the memory limit for duckdb."""
        return self.dcas_config.get('duckdb_memory_limit', None)

    @property
    def dask_threads_number(self):
        """Get the dask threads number."""
        return self.dcas_config.get('dask_threads_number', None)

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

    @property
    def csv_columns(self):
        """Get the columns to be used in the csv output."""
        columns = self.dcas_config.get('csv_columns', None)
        if columns is None:
            return self.default_csv_columns
        return columns

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
            'trigger_error_handling': self.trigger_error_handling,
            'csv_columns': self.csv_columns,
            'duckdb_memory_limit': self.duck_db_memory_limit,
            'dask_threads_number': self.dask_threads_number
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


@shared_task(name="notify_dcas_error")
def notify_dcas_error(date, request_id, error_message):
    """Notify dcas error to the user."""
    # Send an email notification to admins
    admin_emails = get_admin_emails()
    if admin_emails:
        send_mail(
            subject="DCAS Failure Alert",
            message=(
                f"DCAS for request #{request_id} - {date} "
                "has failed.\n\n"
                f"Error: {error_message}\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=admin_emails,
            fail_silently=False,
        )
        logger.info(f"Sent DCAS failure email to {admin_emails}")
    else:
        logger.warning("No admin email found.")


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
    dcas_config = DCASPreferences(dcas_request.requested_at.date())
    dcas_output = DCASPipelineOutput(
        dcas_request.requested_at.date(),
        duck_db_num_threads=dcas_config.duck_db_num_threads,
        duckdb_memory_limit=dcas_config.duck_db_memory_limit
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
            file_path = dcas_output.convert_to_csv(dcas_config.csv_columns)
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
        notify_dcas_error.delay(
            dcas_request.requested_at.date(),
            dcas_request.id,
            'No farm registry group!'
        )
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
        notify_dcas_error.delay(
            dcas_request.requested_at.date(),
            dcas_request.id,
            'No farm registry in the registry groups'
        )
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
        duck_db_num_threads=dcas_config.duck_db_num_threads,
        previous_days_to_check=7,
        dask_num_threads=dcas_config.dask_threads_number,
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
                log_dcas_error.delay(dcas_request.id)
        elif dcas_request.status == TaskStatus.STOPPED:
            # Notify the user about the error
            notify_dcas_error.delay(
                dcas_request.requested_at.date(),
                dcas_request.id,
                errors
            )

        # cleanup
        pipeline.cleanup()


@shared_task(name='log_dcas_error')
def log_dcas_error(request_id, chunk_size=1000):
    """
    Celery task to log farms without messages using chunked queries.

    :param request_id: Id for the pipeline output
    :type request_id: int
    :param chunk_size: Number of rows to process per iteration
    :type chunk_size: int
    """
    conn = None
    try:
        preferences = Preferences.load()
        # Get the most recent DCAS request
        dcas_request = DCASRequest.objects.get(
            id=request_id
        )

        # Initialize pipeline output to get the directory path
        request_date = dcas_request.requested_at.date()
        dcas_output = DCASPipelineOutput(
            request_date,
            duck_db_num_threads=preferences.duckdb_threads_num
        )
        dcas_output._setup_s3fs()
        parquet_path = dcas_output._get_directory_path(
            dcas_output.DCAS_OUTPUT_DIR
        ) + '/iso_a3=*/year=*/month=*/day=*/*.parquet'

        # Clear existing DCASErrorLog
        DCASErrorLog.objects.filter(
            request=dcas_request
        ).delete()

        conn = dcas_output._get_connection(dcas_output.s3)

        # Copy data from parquet to duckdb table
        sql = (f"""
            CREATE TABLE dcas_empty_message AS
            SELECT {dcas_request.id} as request_id,
                registry_id as farm_registry_id,
                'MISSING_MESSAGES' as error_type,
                'Farm registry has no advisory message' as error_message,
                json_array(message, message_2, message_3, message_4,
                message_5, final_message) as messages,
                json_object(
                    'relative_humidity', CAST(humidity AS VARCHAR),
                    'seasonal_precipitation',
                    CAST(seasonal_precipitation AS VARCHAR),
                    'temperature', CAST(temperature AS VARCHAR),
                    'ppet', CAST(p_pet AS VARCHAR),
                    'growth_stage_precipitation',
                    CAST(growth_stage_precipitation AS VARCHAR),
                    'growth_stage_date', growth_stage_start_date,
                    'growth_stage', growth_stage,
                    'prev_week_message', prev_week_message,
                    'total_gdd', total_gdd
                ) as data,
                current_localtimestamp() as logged_at
            FROM read_parquet('{parquet_path}', hive_partitioning=true)
            WHERE year={request_date.year} AND
            month={request_date.month} AND
            day={request_date.day} AND
            is_empty_message = true
        """)
        conn.execute(sql)

        sql = (f"""
            CREATE TABLE dcas_repetitive_message AS
            SELECT {dcas_request.id} as request_id,
                registry_id as farm_registry_id,
                'FOUND_REPETITIVE' as error_type,
                'First message is repetitive message' as error_message,
                json_array(message, message_2, message_3, message_4,
                message_5, final_message) as messages,
                json_object(
                    'relative_humidity', CAST(humidity AS VARCHAR),
                    'seasonal_precipitation',
                    CAST(seasonal_precipitation AS VARCHAR),
                    'temperature', CAST(temperature AS VARCHAR),
                    'ppet', CAST(p_pet AS VARCHAR),
                    'growth_stage_precipitation',
                    CAST(growth_stage_precipitation AS VARCHAR),
                    'growth_stage_date', growth_stage_start_date,
                    'growth_stage', growth_stage,
                    'prev_week_message', prev_week_message,
                    'total_gdd', total_gdd
                ) as data,
                current_localtimestamp() as logged_at
            FROM read_parquet('{parquet_path}', hive_partitioning=true)
            WHERE year={request_date.year} AND
            month={request_date.month} AND
            day={request_date.day} AND
            has_repetitive_message = true
        """)
        conn.execute(sql)

        # get count from dcas_empty_message
        count_sql = """
            SELECT COUNT(*) FROM dcas_empty_message;
        """
        count = conn.execute(count_sql).fetchone()[0]
        logger.info(f"Count of empty messages: {count}")
        if not dcas_request.config:
            dcas_request.config = {}
        dcas_request.config['empty_message_count'] = count

        # get count from dcas_repetitive_message
        count_sql = """
            SELECT COUNT(*) FROM dcas_repetitive_message;
        """
        count = conn.execute(count_sql).fetchone()[0]
        logger.info(f"Count of repetitive messages: {count}")
        dcas_request.config['repetitive_message_count'] = count
        dcas_request.save()

        # enable extension
        conn.install_extension("postgres")
        conn.load_extension("postgres")

        # insert to dcas_error_log table
        pg_conn_str = (
            "host={HOST} port={PORT} user={USER} "
            "password={PASSWORD} dbname={NAME}".format(
                **connection.settings_dict
            )
        )
        conn.execute(f"""
            ATTACH '{pg_conn_str}' AS pg_conn
            (TYPE postgres, SCHEMA 'public');
        """)

        insert_sql = """
            INSERT INTO pg_conn.dcas_error_log
            (request_id, farm_registry_id, error_type, error_message,
            messages, data, logged_at)
            SELECT request_id, farm_registry_id, error_type, error_message,
            messages, data, logged_at
            FROM {};
        """

        conn.execute(insert_sql.format("dcas_empty_message"))
        conn.execute(insert_sql.format("dcas_repetitive_message"))
    except DCASRequest.DoesNotExist:
        logger.error(f"No DCASRequest found for request_id {request_id}.")
    except Exception as e:
        logger.error(f"Error processing dcas error: {str(e)}")
        raise e
    finally:
        if conn:
            conn.close()


@shared_task(name='cleanup_dcas_old_output_files')
def cleanup_dcas_old_output_files():
    """
    Celery task to clean up old DCAS output files.

    This task deletes DCAS output files older than 14 days.
    """
    try:
        # Get the current date
        current_date = timezone.now().date()

        # Calculate the cutoff date (14 days ago)
        cutoff_date = current_date - datetime.timedelta(days=14)

        # Query for old DCAS output files
        old_files = DCASOutput.objects.filter(
            delivered_at__lt=cutoff_date
        )

        # Delete old files
        for file in old_files:
            if file.file_exists:
                remove_dcas_output_file(file.path, file.delivery_by)

    except Exception as e:
        logger.error(f"Error cleaning up old DCAS output files: {str(e)}")


def update_farm_registry_growth_stage_output(request_id):
    """Update farm registry growth stage."""
    logger.info(f"Starting growth stage update for request_id: {request_id}")

    # Step 1: Get DCAS Request
    try:
        dcas_request = DCASRequest.objects.get(id=request_id)
    except DCASRequest.DoesNotExist:
        logger.error(f"DCASRequest with ID {request_id} not found.")
        return

    # Step 2: Update DCASRequest Status to RUNNING
    # TODO: might be better to log the progress into different table
    # dcas_request.status = TaskStatus.RUNNING
    # dcas_request.progress_text = "Growth stage update is in progress..."
    # dcas_request.save()

    # Step 3: Run Growth Stage Update
    try:
        pipeline = DCASDataPipeline(
            dcas_request.farm_registry_group,
            dcas_request.requested_at.date()
        )
        pipeline.update_farm_registry_growth_stage()
        is_success = True
    except Exception as ex:
        logger.error(
            f"Growth stage update failed for request_id {request_id}: {ex}"
        )
        is_success = False

    # Step 4: Update Status in DCASRequest
    dcas_request.end_time = timezone.now()
    dcas_request.status = (
        TaskStatus.COMPLETED if is_success else TaskStatus.STOPPED
    )
    dcas_request.progress_text = (
        "Growth stage update completed successfully!"
        if is_success else "Growth stage update failed."
    )
    dcas_request.save()

    logger.info(
        f"Growth stage update request_id:{request_id}, Success: {is_success}"
    )


@shared_task(name="update_growth_stage_task")
def update_growth_stage_task(request_id):
    """Celery task to update farm registry growth stage."""
    update_farm_registry_growth_stage_output(request_id)


@shared_task(name="clear_all_dcas_error_logs")
def clear_all_dcas_error_logs():
    """Celery task to clear all DCAS error logs."""
    try:
        with connection.cursor() as cursor:
            # Clear all DCAS error logs
            cursor.execute("DELETE FROM dcas_error_log;")
        logger.info("All DCAS error logs cleared successfully.")
    except Exception as e:
        logger.error(
            f"Error clearing DCAS error logs: {str(e)}",
            exc_info=True
        )
        raise e
