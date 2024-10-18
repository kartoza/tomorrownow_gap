# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

import os
import logging
import traceback
from typing import List
from math import ceil
from datetime import datetime, timedelta
from django.contrib.gis.geos import Point
import numpy as np
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import fsspec

from gap.models import (
    Provider,
    Dataset,
    DatasetAttribute,
    DataSourceFile
)
from gap.utils.reader import (
    LocationInputType,
    BaseDatasetReader,
    DatasetReaderInput
)


logger = logging.getLogger(__name__)


class NetCDFProvider:
    """Class contains NetCDF Provider."""

    CBAM = 'CBAM'
    SALIENT = 'Salient'

    @classmethod
    def get_s3_variables(cls, provider: Provider):
        """Get s3 variables for data access.

        :param provider: NetCDF Data Provider
        :type provider: Provider
        :return: Dict<Key, Value> of AWS Credentials
        :rtype: dict
        """
        prefix = provider.name.upper()
        keys = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'AWS_ENDPOINT_URL', 'AWS_BUCKET_NAME',
            'AWS_DIR_PREFIX', 'AWS_REGION_NAME'
        ]
        results = {}
        for key in keys:
            results[key] = os.environ.get(f'{prefix}_{key}', '')
        return results

    @classmethod
    def get_s3_client_kwargs(cls, provider: Provider):
        """Get s3 client_kwargs for s3fs initialization.

        :param provider: NetCDF Data Provider
        :type provider: Provider
        :return: Dict of endpoint_url or region_name
        :rtype: dict
        """
        prefix = provider.name.upper()
        client_kwargs = {}
        if os.environ.get(f'{prefix}_AWS_ENDPOINT_URL', ''):
            client_kwargs['endpoint_url'] = os.environ.get(
                f'{prefix}_AWS_ENDPOINT_URL', '')
        if os.environ.get(f'{prefix}_AWS_REGION_NAME', ''):
            client_kwargs['region_name'] = os.environ.get(
                f'{prefix}_AWS_REGION_NAME', '')
        return client_kwargs


def daterange_inc(start_date: datetime, end_date: datetime):
    """Iterate through start_date and end_date (inclusive).

    :param start_date: start date
    :type start_date: date
    :param end_date: end date inclusive
    :type end_date: date
    :yield: iteration date
    :rtype: date
    """
    days = int((end_date - start_date).days)
    for n in range(days + 1):
        yield start_date + timedelta(n)


def find_start_latlng(metadata: dict) -> float:
    """Find start lat/lng to create coords based on GAP Area of Interest.

    :param metadata: lon_metadata/lat_metadata
    :type metadata: dict
    :return: start of lat/lon to create dataset
    :rtype: float
    """
    diff = ceil(
        abs(
            (metadata['original_min'] - metadata['min']) / metadata['inc']
        )
    )
    return metadata['original_min'] - (diff * metadata['inc'])


class NetCDFMediaS3:
    """Class to provide S3 variables to Media bucket."""

    @classmethod
    def get_s3_variables(cls, dir_name: str) -> dict:
        """Get s3 env variables for NetCDF file.

        :param dir_name: Directory name
        :type dir_name: str
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
            'MINIO_AWS_BUCKET_NAME', '')
        dir_prefix = os.environ.get(
            'MINIO_AWS_DIR_PREFIX', '')
        results['AWS_DIR_PREFIX'] = os.path.join(
            dir_prefix,
            dir_name
        )
        return results

    @classmethod
    def get_s3_client_kwargs(cls) -> dict:
        """Get s3 client kwargs for NetCDF file.

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
    def get_netcdf_base_url(cls, s3: dict) -> str:
        """Generate NetCDF base URL.

        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :return: Base URL with s3 and bucket name
        :rtype: str
        """
        prefix = s3['AWS_DIR_PREFIX']
        bucket_name = s3['AWS_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/{prefix}'
        if not netcdf_url.endswith('/'):
            netcdf_url += '/'
        return netcdf_url


class BaseNetCDFReader(BaseDatasetReader):
    """Base class for NetCDF File Reader."""

    date_variable = 'date'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime,
            altitudes: (float, float) = None
    ) -> None:
        """Initialize BaseNetCDFReader class.

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
        :param altitudes: Altitudes for the reader
        :type altitudes: (float, float)
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.xrDatasets = []

    def setup_reader(self):
        """Initialize s3fs."""
        self.s3 = NetCDFProvider.get_s3_variables(self.dataset.provider)
        self.fs = fsspec.filesystem(
            's3',
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=(
                NetCDFProvider.get_s3_client_kwargs(self.dataset.provider)
            )
        )

    def open_dataset(self, source_file: DataSourceFile) -> xrDataset:
        """Open a NetCDFFile using xArray.

        :param source_file: NetCDF from a dataset
        :type source_file: DataSourceFile
        :return: xArray Dataset object
        :rtype: xrDataset
        """
        prefix = self.s3['AWS_DIR_PREFIX']
        bucket_name = self.s3['AWS_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/{prefix}'
        if not netcdf_url.endswith('/'):
            netcdf_url += '/'
        netcdf_url += f'{source_file.name}'
        return xr.open_dataset(self.fs.open(netcdf_url))

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _has_ensembles(self):
        attr = [a for a in self.attributes if a.ensembles]
        return len(attr) > 0

    def read_variables(
            self, dataset: xrDataset, start_date: datetime = None,
            end_date: datetime = None) -> xrDataset:
        """Read data from list variable with filter from given Point.

        :param dataset: xArray Dataset object
        :type dataset: xrDataset
        :return: filtered xArray Dataset object
        :rtype: xrDataset
        """
        start_dt = np.datetime64(start_date, 'ns')
        end_dt = np.datetime64(end_date, 'ns')
        variables = [a.source for a in self.attributes]
        variables.append(self.date_variable)
        if self._has_ensembles():
            variables.append('ensemble')
        result: xrDataset = None
        try:
            if self.location_input.type == LocationInputType.BBOX:
                result = self._read_variables_by_bbox(
                    dataset, variables, start_dt, end_dt)
            elif self.location_input.type == LocationInputType.POLYGON:
                result = self._read_variables_by_polygon(
                    dataset, variables, start_dt, end_dt)
            elif self.location_input.type == LocationInputType.LIST_OF_POINT:
                result = self._read_variables_by_points(
                    dataset, variables, start_dt, end_dt)
            else:
                result = self._read_variables_by_point(
                    dataset, variables, start_dt, end_dt)
        except Exception as ex:
            logger.error(
                'Failed to read_variables from '
                f'netcdf dataset {self.dataset.provider.name} '
                f'date {start_date} - {end_date} with vars: {variables}'
            )
            logger.error(ex)
            logger.error(traceback.format_exc())
        return result

    def find_locations(self, val: xrDataset) -> List[Point]:
        """Find locations from dataset.

        :param val: dataset to be read
        :type val: xrDataset
        :return: points
        :rtype: List[Point]
        """
        locations = []
        lat_values = val['lat'].values
        lon_values = val['lon'].values
        if lat_values.ndim == 0 and lon_values.ndim == 0:
            return [Point(x=float(lon_values), y=float(lat_values))], 1, 1
        for lat in lat_values:
            for lon in lon_values:
                locations.append(Point(x=float(lon), y=float(lat)))
        return locations, len(lat_values), len(lon_values)
