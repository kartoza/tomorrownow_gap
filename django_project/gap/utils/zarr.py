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
import numpy as np
from xarray.core.dataset import Dataset as xrDataset
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.models import ObjectStorageManager
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
            use_cache: bool = True
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
            dataset, attributes, location_input,
            start_date, end_date, use_cache
        )

    @classmethod
    def get_s3_variables(cls) -> dict:
        """Get s3 env variables for Zarr file.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        results = ObjectStorageManager.get_s3_env_vars()
        return results

    @classmethod
    def get_s3_client_kwargs(cls, s3: dict = None) -> dict:
        """Get s3 client kwargs for Zarr file.

        :return: dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        if s3 is None:
            s3 = cls.get_s3_variables()
        client_kwargs = {}
        if s3.get('S3_ENDPOINT_URL'):
            client_kwargs['endpoint_url'] = s3['S3_ENDPOINT_URL']
        if s3.get('S3_REGION_NAME'):
            client_kwargs['region_name'] = s3['S3_REGION_NAME']
        return client_kwargs

    @classmethod
    def get_zarr_base_url(cls, s3: dict) -> str:
        """Generate Zarr base URL.

        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :return: Base URL with s3 and bucket name
        :rtype: str
        """
        prefix = s3['S3_DIR_PREFIX']
        bucket_name = s3['S3_BUCKET_NAME']
        zarr_url = f's3://{bucket_name}/{prefix}'
        if not zarr_url.endswith('/'):
            zarr_url += '/'
        return zarr_url

    @classmethod
    def get_zarr_cache_dir(cls, data_source: DataSourceFile) -> str:
        """Get the directory for zarr cache.

        :param data_source: DataSourceFile instance
        :type data_source: DataSourceFile
        :return: Path to the zarr cache
        :rtype: str
        """
        hostname = cls.get_reader_hostname()
        cache_filename = hostname.replace('.', '_') + f'_{data_source.id}'
        cache_filename = cache_filename.replace('/', '_')
        return f'/tmp/{cache_filename}'

    @classmethod
    def get_reader_hostname(cls) -> str:
        """Get the hostname with process ID of the reader.

        :return: Hostname of the reader
        :rtype: str
        """
        # Using os.uname() to get the hostname
        hostname = os.uname()[1]
        # Append the process ID to the hostname
        # because we run multiple workers in 1 container
        hostname += f'_{os.getpid()}'
        return hostname

    def setup_reader(self):
        """Initialize s3fs."""
        self.s3 = self.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('S3_ACCESS_KEY_ID'),
            'secret': self.s3.get('S3_SECRET_ACCESS_KEY'),
            'client_kwargs': self.get_s3_client_kwargs(
                s3=self.s3
            )
        }

    def open_dataset(self, source_file: DataSourceFile) -> xrDataset:
        """Open a zarr file using xArray.

        :param source_file: zarr file from a dataset
        :type source_file: DataSourceFile
        :return: xArray Dataset object
        :rtype: xrDataset
        """
        metadata = source_file.metadata or {}
        use_cache = metadata.get('use_cache', self.use_cache)
        if use_cache:
            self._check_zarr_cache_expiry(source_file)

        s3 = self.s3
        s3_options = self.s3_options
        override_conn_name = metadata.get(
            'connection_name', None
        )
        if (
            override_conn_name and
            override_conn_name != s3.get('S3_CONNECTION_NAME')
        ):
            # if there is a connection name override, we need to get the
            # s3 variables from the ObjectStorageManager
            s3 = ObjectStorageManager.get_s3_env_vars(
                connection_name=override_conn_name
            )
            s3_options = {
                'key': s3.get('S3_ACCESS_KEY_ID'),
                'secret': s3.get('S3_SECRET_ACCESS_KEY'),
                'client_kwargs': self.get_s3_client_kwargs(
                    s3=s3
                )
            }

        # get zarr url
        zarr_url = self.get_zarr_base_url(s3)
        zarr_url += f'{source_file.name}'

        if use_cache:
            # create s3 filecache
            s3_fs = s3fs.S3FileSystem(
                **s3_options,
            )
            fs = fsspec.filesystem(
                'filecache',
                target_protocol='s3',
                target_options=s3_options,
                cache_storage=self.get_zarr_cache_dir(source_file),
                cache_check=3600,
                expiry_time=86400,
                target_kwargs={
                    's3': s3_fs
                }
            )
            # create fsspec mapper file list
            s3_mapper = fs.get_mapper(zarr_url)
        else:
            s3_mapper = fsspec.get_mapper(zarr_url, **s3_options)

        drop_variables = metadata.get('drop_variables', [])
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
        hostname = self.get_reader_hostname()
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
                    created_on=timezone.now(),
                    cache_dir=self.get_zarr_cache_dir(source_file)
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
        cache_dir = self.get_zarr_cache_dir(source_file)
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)

    def _mask_by_datetime_range(
        self, val: xrDataset,
        start_dt: np.datetime64,
        end_dt: np.datetime64
    ) -> xrDataset:
        """Mask the dataset by datetime range.

        :param val: xArray Dataset object
        :type val: xrDataset
        :param start_dt: Start datetime for filtering
        :type start_dt: np.datetime64
        :param end_dt: End datetime for filtering
        :type end_dt: np.datetime64
        :return: Filtered xArray Dataset object
        :rtype: xrDataset
        """
        date_b, time_b = xr.broadcast(val['date'], val['time'])
        full_datetime = date_b + time_b
        mask = (full_datetime >= start_dt) & \
            (full_datetime <= end_dt)
        val = val.where(mask, drop=False)
        return val
