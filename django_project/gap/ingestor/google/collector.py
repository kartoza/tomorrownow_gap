# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Collector for Google Nowcast Dataset.
"""

import logging
from datetime import datetime
import os
from django.utils import timezone
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage

from core.utils.gdrive import (
    gdrive_file_list
)
from gap.models import (
    Dataset,
    DatasetStore,
    DataSourceFile
)
from gap.ingestor.base import (
    BaseIngestor
)
from gap.ingestor.google.common import (
    get_forecast_target_time_from_filename
)

logger = logging.getLogger(__name__)
GEE_DEFAULT_FOLDER_NAME = 'EarthEngineExports'
DEFAULT_UPLOAD_DIR = 'google_nowcast_collector'


class GoogleNowcastCollector(BaseIngestor):
    """Collector for Google Nowcast data."""

    def __init__(self, session, working_dir='/tmp'):
        """Initialize GoogleNowcastCollector."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()

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

        folder_name = self.get_config(
            'gdrive_folder_name',
            GEE_DEFAULT_FOLDER_NAME
        )
        files = gdrive_file_list(folder_name)
        if not files:
            logger.error(f"No files found in folder: {folder_name}")
            return []

        logger.info(f"Found {len(files)} files in folder: {folder_name}")
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

    def _run(self):
        """Run the collector to fetch and process data."""
        logger.info("Starting Google Nowcast data collection.")

        # Step 1: Run GEE Script to fetch latest timestamp
        # Step 2: Run GEE Script to fetch data for the latest timestamp
        # Step 3: Wait until all export tasks are complete
        # Step 4: Fetch the exported data from Gdrive
        exported_files = self._fetch_exported_files()
        # Step 5: cleanup the files from Gdrive
        self._clean_gdrive_files(exported_files)

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
