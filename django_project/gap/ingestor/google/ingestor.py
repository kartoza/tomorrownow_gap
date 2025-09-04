# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor for Google Nowcast Dataset.
"""

from datetime import datetime, timezone
import logging
import os
import json
import time
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.utils import timezone as dtimezone

from gap.models import (
    Dataset, DatasetStore, IngestorSessionStatus, CollectorSession,
    DatasetAttribute, DataSourceFile
)
from core.utils.s3 import s3_compatible_env
from gap.utils.zarr import BaseZarrReader
from gap.ingestor.base import BaseZarrIngestor
from gap.ingestor.exceptions import (
    MissingCollectorSessionException,
    FileNotFoundException
)
from gap.utils.dask import execute_dask_compute
from gap.ingestor.google.cog import cog_to_xarray_advanced

logger = logging.getLogger(__name__)


class GoogleNowcastIngestor(BaseZarrIngestor):
    """Ingestor for Google Nowcast Dataset."""

    default_chunks = {
        'time': 50,
        'lat': 150,
        'lon': 110
    }

    def __init__(self, session, working_dir):
        """Initialize GoogleNowcastIngestor."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()
        # init variables
        self.variables = list(
            DatasetAttribute.objects.filter(
                dataset=self.dataset
            ).values_list(
                'source', flat=True
            )
        )

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Google Nowcast | 12-hour Forecast',
            store_type=DatasetStore.ZARR
        )

    def _get_encoding(self):
        """Get encoding for dataset variables."""
        encoding = {
            'time': {
                'chunks': self.default_chunks['time'],
                'units': 'seconds since 1970-01-01T00:00:00',  # Unix epoch
                'dtype': 'int64'
            }
        }
        for var in self.variables:
            encoding[var] = {
                'chunks': (
                    self.default_chunks['time'],
                    self.default_chunks['lat'],
                    self.default_chunks['lon']
                )
            }
        return encoding

    def _apply_transformation(self, ds):
        """Apply transformations to the dataset."""
        # no transformation for google nowcast dataset
        return ds

    def _run(self):
        """Process the Google Nowcast dataset."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)

        data_sources = collector.dataset_files.all().order_by(
            'metadata__forecast_target_time'
        )
        if data_sources.count() == 0:
            logger.warning(
                f"No data sources found for collector {collector.id} "
                f"in session {self.session.id}"
            )
            raise FileNotFoundException()

        for data_source in data_sources:
            remote_path = data_source.metadata['remote_url']
            remote_path = (
                f"s3://{self.s3.get('S3_BUCKET_NAME')}/{remote_path}"
            )
            forecast_target_time = data_source.metadata['forecast_target_time']

            logger.info(f"Processing file: {remote_path}")
            start_time = time.time()
            progress = self._add_progress(os.path.basename(remote_path))
            try:
                with s3_compatible_env(
                    access_key=self.s3.get('S3_ACCESS_KEY_ID'),
                    secret_key=self.s3.get('S3_SECRET_ACCESS_KEY'),
                    endpoint_url=self.s3.get('S3_ENDPOINT_URL'),
                    region=self.s3.get('S3_REGION_NAME')
                ):
                    ds = cog_to_xarray_advanced(
                        remote_path,
                        chunks='auto',
                        reproject_to_wgs84=True,
                        separate_bands=True,
                        band_names=None,
                        verbose=self.get_config('verbose', False),
                        add_variable_metadata=self.created,
                        forecast_target_time=forecast_target_time
                    )

                    ds = self._apply_transformation(ds)

                    # save the dataset to Zarr
                    zarr_url = (
                        BaseZarrReader.get_zarr_base_url(self.s3) +
                        self.datasource_file.name
                    )
                    if self.created:
                        x = ds.to_zarr(
                            zarr_url,
                            mode='w',
                            consolidated=True,
                            encoding=self._get_encoding(),
                            storage_options=self.s3_options,
                            compute=False
                        )
                        self.created = False
                    else:
                        x = ds.to_zarr(
                            zarr_url,
                            mode='a',
                            append_dim='time',
                            consolidated=True,
                            storage_options=self.s3_options,
                            compute=False
                        )

                # execute dask compute to finalize the dataset
                execute_dask_compute(x)

                # update progress
                total_time = time.time() - start_time
                progress.notes = f"Execution time: {total_time}"
                progress.status = IngestorSessionStatus.SUCCESS
                progress.save()
            except Exception as e:
                logger.error(f"Failed to process {remote_path}: {e}")
                progress.notes = str(e)
                progress.status = IngestorSessionStatus.FAILED
                progress.save()
                raise e

        # update start/end datetime of zarr datasource file
        self.datasource_file.start_date_time = datetime.fromtimestamp(
            data_sources.first().metadata['forecast_target_time'],
            tz=timezone.utc
        )
        self.datasource_file.end_date_time = datetime.fromtimestamp(
            data_sources.last().metadata['forecast_target_time'],
            tz=timezone.utc
        )
        self.datasource_file.save()

        # remove temporary source file
        remove_temp_file = self.get_config('remove_temp_file', True)
        if remove_temp_file:
            self._remove_source_files(collector)

        # set data source retention
        self.set_data_source_retention()

    def _remove_source_files(self, collector: CollectorSession):
        s3_storage: S3Boto3Storage = storages["gap_products"]
        for dataset_file in collector.dataset_files.all():
            remote_path = dataset_file.metadata['remote_url']
            s3_storage.delete(remote_path)
            dataset_file.delete()

    def run(self):
        """Run Google NowCast Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error(
                f'Ingestor {self.dataset.name} failed!',
                exc_info=True
            )
            raise e

    def set_data_source_retention(self):
        """Delete the latest data source file and set the new one."""
        # find the latest data source file
        latest_data_source = DataSourceFile.objects.filter(
            dataset=self.dataset,
            is_latest=True,
            format=DatasetStore.ZARR
        ).last()
        if (
            latest_data_source and
            latest_data_source.id != self.datasource_file.id
        ):
            # set the latest data source file to not latest
            latest_data_source.is_latest = False
            # set deleted_at to now
            latest_data_source.deleted_at = dtimezone.now()
            latest_data_source.save()

        # Update the data source file to latest
        self.datasource_file.is_latest = True
        self.datasource_file.save()


class GoogleGraphcastIngestor(GoogleNowcastIngestor):
    """Ingestor for Google Graphcast Dataset."""

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Google Graphcast | 10-day Forecast',
            store_type=DatasetStore.ZARR
        )

    def _apply_transformation(self, ds):
        """Apply transformations to the dataset."""
        # Convert temperature from Kelvin to Celsius
        ds['2m_temperature'] = ds['2m_temperature'] - 273.15
        return ds
