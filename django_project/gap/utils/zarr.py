# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading Zarr File
"""

import os
import logging
import s3fs
import fsspec
import shutil
from typing import List
from datetime import datetime
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
from django.db import IntegrityError, transaction
from django.utils import timezone

from gap.models import (
    Dataset,
    DatasetAttribute,
    DataSourceFile,
    DataSourceFileCache
)
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.utils.netcdf import BaseNetCDFReader


logger = logging.getLogger(__name__)


class BaseZarrReader(BaseNetCDFReader):
    """Base class for Zarr Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime,
            altitudes: (float, float) = None
    ) -> None:
        """Initialize BaseZarrReader class.

        :param dataset: Dataset for reading
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )

    @classmethod
    def get_s3_variables(cls) -> dict:
        """Get s3 env variables for Zarr file.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        prefix = 'MINIO'
        keys = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'AWS_ENDPOINT_URL', 'AWS_REGION_NAME'
        ]
        results = {}
        for key in keys:
            results[key] = os.environ.get(f'{prefix}_{key}', '')
        results['AWS_BUCKET_NAME'] = os.environ.get(
            'MINIO_GAP_AWS_BUCKET_NAME', '')
        results['AWS_DIR_PREFIX'] = os.environ.get(
            'MINIO_GAP_AWS_DIR_PREFIX', '')
        return results

    @classmethod
    def get_s3_client_kwargs(cls) -> dict:
        """Get s3 client kwargs for Zarr file.

        :return: dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        prefix = 'MINIO'
        client_kwargs = {}
        if os.environ.get(f'{prefix}_AWS_ENDPOINT_URL', ''):
            client_kwargs['endpoint_url'] = os.environ.get(
                f'{prefix}_AWS_ENDPOINT_URL', '')
        if os.environ.get(f'{prefix}_AWS_REGION_NAME', ''):
            client_kwargs['region_name'] = os.environ.get(
                f'{prefix}_AWS_REGION_NAME', '')
        return client_kwargs

    @classmethod
    def get_zarr_base_url(cls, s3: dict) -> str:
        """Generate Zarr base URL.

        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :return: Base URL with s3 and bucket name
        :rtype: str
        """
        prefix = s3['AWS_DIR_PREFIX']
        bucket_name = s3['AWS_BUCKET_NAME']
        zarr_url = f's3://{bucket_name}/{prefix}'
        if not zarr_url.endswith('/'):
            zarr_url += '/'
        return zarr_url

    @classmethod
    def get_zarr_cache_dir(cls, zarr_filename: str) -> str:
        """Get the directory for zarr cache.

        :param zarr_filename: DataSourceFile name
        :type zarr_filename: str
        :return: Path to the zarr cache
        :rtype: str
        """
        cache_filename = zarr_filename.replace('.', '_')
        cache_filename = cache_filename.replace('/', '_')
        return f'/tmp/{cache_filename}'

    def setup_reader(self):
        """Initialize s3fs."""
        self.s3 = self.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self.get_s3_client_kwargs()
        }

    def open_dataset(self, source_file: DataSourceFile) -> xrDataset:
        """Open a zarr file using xArray.

        :param source_file: zarr file from a dataset
        :type source_file: DataSourceFile
        :return: xArray Dataset object
        :rtype: xrDataset
        """
        self._check_zarr_cache_expiry(source_file)
        # get zarr url
        zarr_url = self.get_zarr_base_url(self.s3)
        zarr_url += f'{source_file.name}'

        # create s3 filecache
        s3_fs = s3fs.S3FileSystem(
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            endpoint_url=self.s3.get('AWS_ENDPOINT_URL')
        )
        fs = fsspec.filesystem(
            'filecache',
            target_protocol='s3',
            target_options=self.s3_options,
            cache_storage=self.get_zarr_cache_dir(source_file.name),
            cache_check=3600,
            expiry_time=86400,
            target_kwargs={
                's3': s3_fs
            }
        )

        # create fsspec mapper file list
        s3_mapper = fs.get_mapper(zarr_url)
        drop_variables = []
        if source_file.metadata:
            drop_variables = source_file.metadata.get(
                'drop_variables', [])
        # open zarr, use consolidated to read the metadata
        ds = xr.open_zarr(
            s3_mapper, consolidated=True, drop_variables=drop_variables)

        return ds

    def _check_zarr_cache_expiry(self, source_file: DataSourceFile):
        """Validate cache directory for zarr.

        The cache dir will be cleared if there is update from ingestor.
        :param source_file: zarr source file
        :type source_file: DataSourceFile
        """
        hostname = os.uname()[1]
        cache_row = DataSourceFileCache.objects.filter(
            source_file=source_file,
            hostname=hostname
        ).first()
        if cache_row is None:
            # no existing record yet, create first
            try:
                DataSourceFileCache.objects.create(
                    source_file=source_file,
                    hostname=hostname,
                    created_on=timezone.now()
                )
                self.clear_cache(source_file)
            except IntegrityError:
                pass
        elif cache_row.expired_on:
            # when there is expired_on, we should remove the cache dir
            with transaction.atomic():
                update_row = (
                    DataSourceFileCache.objects.select_for_update().get(
                        id=cache_row.id
                    )
                )
                self.clear_cache(source_file)
                update_row.expired_on = None
                update_row.save()

    def clear_cache(self, source_file: DataSourceFile):
        """Clear cache of zarr file.

        :param source_file: DataSourceFile for the zarr
        :type source_file: DataSourceFile
        """
        cache_dir = self.get_zarr_cache_dir(source_file.name)
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
