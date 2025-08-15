# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Collector for Google Nowcast Dataset.
"""

import logging
from datetime import datetime
import os
import time
import json
from django.utils import timezone
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage

from core.utils.gee import (
    initialize_earth_engine
)
from core.utils.gdrive import (
    gdrive_file_list,
    get_gdrive_file
)
from gap.models import (
    Dataset,
    DatasetStore,
    DataSourceFile,
    IngestorSessionStatus
)
from gap.ingestor.base import (
    BaseIngestor
)
from gap.ingestor.google.common import (
    get_forecast_target_time_from_filename
)
from gap.ingestor.google.gee import (
    get_latest_nowcast_timestamp,
    extract_nowcast_at_timestamp
)

logger = logging.getLogger(__name__)
GEE_DEFAULT_FOLDER_NAME = 'GAPNowcastEEExportstest'
DEFAULT_UPLOAD_DIR = 'google_nowcast_collector'
GEE_DEFAULT_WAIT_SLEEP = 5  # seconds


class GoogleNowcastCollector(BaseIngestor):
    """Collector for Google Nowcast data."""

    def __init__(self, session, working_dir='/tmp'):
        """Initialize GoogleNowcastCollector."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()
        self.date = self.get_config('date', timezone.now().date())
        self.verbose = self.get_config('verbose', False)
        self.sleep_time = self.get_config(
            'sleep_time', GEE_DEFAULT_WAIT_SLEEP
        )
        self.folder_name = self.get_config(
            'gdrive_folder_name',
            GEE_DEFAULT_FOLDER_NAME
        )
        self.bucket_name = 'gap-prd-bkt-gcs-01'

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Google Nowcast | 12-hour Forecast',
            store_type=DatasetStore.ZARR
        )

    def _fetch_exported_files(self):
        """Fetch exported files from Google Drive."""
        # reset existing dataset_files
        self.session.dataset_files.all().delete()
        self.session.dataset_files.clear()

        files = gdrive_file_list(self.folder_name)
        if not files:
            logger.error(f"No files found in folder: {self.folder_name}")
            return []

        logger.info(f"Found {len(files)} files in folder: {self.folder_name}")
        results = {}
        for file in files:
            filename = file['title']
            try:
                # Extract forecast target time epoch seconds from filename
                forecast_target_time = get_forecast_target_time_from_filename(
                    filename
                )
            except ValueError as e:
                logger.error(f"Invalid filename {filename}: {e}")
                continue

            results[forecast_target_time] = file

        # Sort results by forecast target time ascending
        forecast_target_times = sorted(results.keys())
        data_sources = []
        for forecast_target_time in forecast_target_times:
            file = results[forecast_target_time]
            filename = file['title']

            # Construct remote URL for the file
            remote_url = os.path.join(
                self.s3.get('S3_DIR_PREFIX'),
                DEFAULT_UPLOAD_DIR,
                filename
            )

            # Download the file to the working directory
            file_path = os.path.join(self.working_dir, filename)
            file.GetContentFile(file_path)
            # get file size
            file_size = os.path.getsize(file_path)

            # Upload the file to S3
            s3_storage: S3Boto3Storage = storages["gap_products"]
            # check if file exists
            if s3_storage.exists(remote_url):
                # delete existing file
                s3_storage.delete(remote_url)

            with open(file_path, 'rb') as f:
                s3_storage.save(remote_url, f)

            # Create DataSourceFile
            data_sources.append(
                DataSourceFile.objects.create(
                    name=filename,
                    dataset=self.dataset,
                    start_date_time=datetime.fromtimestamp(
                        forecast_target_time,
                        tz=timezone.utc
                    ),
                    end_date_time=datetime.fromtimestamp(
                        forecast_target_time,
                        tz=timezone.utc
                    ),
                    format=DatasetStore.COG,
                    created_on=timezone.now(),
                    metadata={
                        'forecast_target_time': forecast_target_time,
                        'remote_url': remote_url,
                        'file_size': file_size
                    }
                )
            )

        # set data sources to session
        self.session.dataset_files.set(data_sources)

        return list(results.values())

    def _clean_gdrive_files(self, files):
        """Clean up files from Google Drive.

        :param files: List of files to clean up
        """
        remove_temp_file = self.get_config(
            'remove_temp_file', True
        )
        if not remove_temp_file:
            return

        for file in files:
            try:
                file.Delete()
            except Exception as e:
                logger.error(f"Error deleting file {file.name}: {e}")

    def _start_task(self, task):
        """Start an export task if there is no exported file."""
        file_name = task['file_name']
        # gdrive_file = get_gdrive_file(file_name)
        # if gdrive_file:
        #     logger.info(f"File {file_name} already exists in Google Drive.")
        #     return None

        if self.verbose:
            logger.info(f"Starting export task for {file_name}")
        task['task'].start()
        task['start_time'] = time.time()
        task['progress'] = self._add_progress(file_name)
        task['progress'].status = IngestorSessionStatus.RUNNING
        task['progress'].save()

        return task

    def _run(self):
        """Run the collector to fetch and process data."""
        logger.info(f"Starting Google Nowcast data collection {self.date}.")

        # Step 1: Run GEE Script to fetch latest timestamp
        initialize_earth_engine()
        latest_timestamp = get_latest_nowcast_timestamp(
            self.date
        )
        if self.verbose:
            # convert timestamp to human-readable format
            latest_timestamp_str = datetime.fromtimestamp(
                latest_timestamp, tz=timezone.utc
            ).strftime('%Y-%m-%d %H:%M:%S')
            logger.info(
                f"Latest Nowcast timestamp for {self.date}: "
                f"{latest_timestamp_str}"
            )

        # Step 2: Run GEE Script to fetch data for the latest timestamp
        tasks = extract_nowcast_at_timestamp(
            latest_timestamp, self.bucket_name,
            verbose=self.verbose
        )

        # Step 3: Wait until all export tasks are complete
        if self.verbose:
            logger.info(
                f"Submitting {len(tasks)} export tasks and "
                "waiting to complete."
            )
        started_tasks = []
        for task in tasks:
            task = self._start_task(task)
            if not task:
                continue

            started_tasks.append(task)

        for task in started_tasks:
            while task['task'].active():
                status = task['task'].status()
                time.sleep(self.sleep_time)

            status = task['task'].status()
            if self.verbose:
                logger.info(
                    f"Task {task['file_name']} completed with status: "
                    f"{status['state']}"
                )

            # Update progress
            task['elapsed_time'] = time.time() - task['start_time']
            if status['state'] == 'COMPLETED':
                task['progress'].status = IngestorSessionStatus.SUCCESS
                task['progress'].notes = (
                    f'Task completed in {task["elapsed_time"]:.2f} seconds'
                )
            else:
                task['progress'].status = IngestorSessionStatus.FAILED
                task['progress'].notes = json.dumps(status, default=str)
            task['progress'].save()

        # count failed tasks
        failed_tasks = [
            task for task in started_tasks
            if task['progress'].status == IngestorSessionStatus.FAILED
        ]
        if failed_tasks:
            raise Exception(
                f"Some tasks failed: {len(failed_tasks)} out of "
                f"{len(started_tasks)} tasks."
            )
        else:
            logger.info("All tasks completed successfully.")

        # # Step 4: Fetch the exported data from Gdrive
        # exported_files = self._fetch_exported_files()
        # # Step 5: cleanup the files from Gdrive
        # self._clean_gdrive_files(exported_files)

    def run(self):
        """Run the collector."""
        try:
            self._run()
        except Exception as e:
            logger.error(
                f"Error running Google Nowcast Collector: {e}",
                exc_info=True
            )
            raise e
