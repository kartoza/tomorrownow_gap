# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term ingestor using json.
"""

import json
import logging
import os
import traceback
import uuid
from datetime import timedelta

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.utils import timezone

from core.utils.s3 import zip_folder_in_s3
from gap.ingestor.base import (
    BaseIngestor
)
from gap.models import (
    CastType, CollectorSession, DataSourceFile, DatasetStore, Grid
)
from gap.providers import TomorrowIODatasetReader
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset
from gap.utils.reader import DatasetReaderInput


logger = logging.getLogger(__name__)


def path(filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}tio-short-term-collector/{filename}'


class TioShortTermCollector(BaseIngestor):
    """Collector for Tio Short Term data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioShortTermCollector."""
        super().__init__(session, working_dir)
        self.dataset = tomorrowio_shortterm_forecast_dataset()
        today = timezone.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Retrieve D-6 to D+14
        # Total days: 21
        self.start_dt = today - timedelta(days=6)
        self.end_dt = today + timedelta(days=15)
        self.forecast_date = today

    def _run(self):
        """Run TomorrowIO ingestor."""
        s3_storage = default_storage
        zip_file = path(f"{uuid.uuid4()}.zip")
        dataset = self.dataset
        start_dt = self.start_dt
        end_dt = self.end_dt
        data_source_file, _ = DataSourceFile.objects.get_or_create(
            dataset=dataset,
            start_date_time=start_dt,
            end_date_time=end_dt,
            format=DatasetStore.ZIP_FILE,
            defaults={
                'name': zip_file,
                'created_on': timezone.now(),
                'metadata': {
                    'forecast_date': self.forecast_date.date().isoformat()
                }
            }
        )
        filename = data_source_file.name.split('/')[-1]
        _uuid = os.path.splitext(filename)[0]
        zip_file = path(f"{_uuid}.zip")
        folder = path(_uuid)

        # If it is already have zip file, skip the process
        if s3_storage.exists(zip_file):
            return

        TomorrowIODatasetReader.init_provider()
        for grid in Grid.objects.all():
            file_name = f"grid-{grid.id}.json"
            bbox_filename = os.path.join(folder, file_name)

            # If the json file is exist, skip it
            if s3_storage.exists(bbox_filename):
                continue

            # Get the data
            location_input = DatasetReaderInput.from_point(
                grid.geometry.centroid
            )
            forecast_attrs = dataset.datasetattribute_set.filter(
                dataset__type__type=CastType.FORECAST
            )
            reader = TomorrowIODatasetReader(
                dataset,
                forecast_attrs,
                location_input, start_dt, end_dt
            )
            reader.read()
            values = reader.get_data_values()

            # Save the reasult to file
            content = ContentFile(
                json.dumps(values.to_json(), separators=(',', ':')))
            s3_storage.save(bbox_filename, content)

        # Zip the folder
        zip_folder_in_s3(
            s3_storage, folder_path=folder, zip_file_name=zip_file
        )

        # Add data source file to collector result
        self.session.dataset_files.set([data_source_file])

    def run(self):
        """Run Tio Short Term Ingestor."""
        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            logger.error('Ingestor Tio Short Term failed!')
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass
