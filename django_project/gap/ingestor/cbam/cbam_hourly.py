# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Hourly ingestor.
"""

import os
import json
import logging
import pytz
import s3fs
import tempfile
import traceback
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da
import time
import datetime
from datetime import date
from django.utils import timezone
from typing import List
from xarray.core.dataset import Dataset as xrDataset

from core.models import ObjectStorageManager
from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, IngestorSessionStatus,
    CollectorSession, DatasetAttribute
)
from gap.utils.netcdf import find_start_latlng
from gap.utils.zarr import BaseZarrReader
from gap.ingestor.base import BaseIngestor, BaseZarrIngestor, CoordMapping
from gap.utils.dask import execute_dask_compute
from gap.ingestor.exceptions import MissingCollectorSessionException


logger = logging.getLogger(__name__)


class CBAMHourlyCollector(BaseIngestor):
    """Data collector for CBAM Hourly data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize CBAMHourlyCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(
            name='CBAM Reanalysis Hourly | 2020-2022')
        # S3 connection to the NetCDF files
        self.fs = s3fs.S3FileSystem(**self.s3_options)

        self.dir_paths = session.additional_config.get('directory_paths', [])
        if not self.dir_paths:
            raise RuntimeError('Collector session must have directory_paths!')
        self.total_count = 0
        self.data_files = []

    def _parse_filename(self, filename):
        """Parse NETCDF filename: 20200221T060000Z.nc."""
        cleaned_name = filename.split('.')[0]

        file_date = datetime.datetime.strptime(
            cleaned_name, '%Y%m%dT%H%M%SZ'
        )
        return file_date

    def _walk_s3_directory(self, bucket_name: str, dir_path: str):
        total_count = 0
        print(f'Walking S3 directory: {dir_path}')
        for dirpath, dirnames, filenames in \
            self.fs.walk(f's3://{bucket_name}/{dir_path}'):
            # check if cancelled
            if self.is_cancelled():
                break

            # iterate for each file
            for filename in filenames:
                # check if cancelled
                if self.is_cancelled():
                    break

                # skip non-nc file
                if not filename.endswith('.nc'):
                    continue

                # check existing and skip if exist
                check_exist = DataSourceFile.objects.filter(
                    name=filename,
                    dataset=self.dataset,
                    format=DatasetStore.NETCDF
                ).exists()
                if check_exist:
                    continue

                # parse datetime from filename
                file_date = self._parse_filename(filename)
                # set timezone to UTC
                file_date = file_date.replace(tzinfo=pytz.UTC)

                # insert record to DataSourceFile
                self.data_files.append(DataSourceFile.objects.create(
                    name=filename,
                    dataset=self.dataset,
                    start_date_time=file_date,
                    end_date_time=file_date,
                    created_on=timezone.now(),
                    format=DatasetStore.NETCDF,
                    metadata={
                        'directory_path': dir_path,
                        's3_connection_name': self.s3.get(
                            'S3_CONNECTION_NAME',
                            None
                        ),
                    }
                ))
                total_count += 1
        return total_count

    def _run(self):
        """Collect list of files in the CBAM S3 directory.

        The bias adjusted is currently hosted on TNGAP Products.
        The NetCDF Files are for a year and just contain 1 attribute.
        The file structure must be:
            '{attribute}/{attribute}_interpolated_YYYY_RBF.nc'
        """
        logger.info(f'Check NETCDF Files by dataset {self.dataset.name}')

        bucket_name = self.s3.get('S3_BUCKET_NAME')
        self.total_count = 0
        for dir_path in self.dir_paths:
            full_dir_path = os.path.join(
                self.s3['S3_DIR_PREFIX'], dir_path
            )
            total_count = self._walk_s3_directory(
                bucket_name=bucket_name,
                dir_path=full_dir_path
            )
            self.total_count += total_count
            logger.info(
                f'Found {total_count} new NetCDFFile in {full_dir_path}'
            )
            # check if cancelled
            if self.is_cancelled():
                logger.info('Collector CBAM Hourly cancelled!')
                break

        if self.total_count > 0:
            logger.info(
                f'{self.dataset.name} - Added new NetCDFFile: '
                f'{self.total_count}'
            )
            self.session.dataset_files.set(self.data_files)

    def run(self):
        """Run CBAM Bias Adjust Data Collector."""
        try:
            self._run()
        except Exception as e:
            logger.error('Collector CBAM failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass


class CBAMHourlyIngestor(BaseZarrIngestor):
    """Ingestor CBAM Hourly dataset into Zarr."""

    default_chunks = {
        'date': 7,
        'time': 24,
        'lat': 150,
        'lon': 150
    }
    variables = []
    DATE_VARIABLE = 'date'
    DEFAULT_VAR_CHUNK_SIZE = 20

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize CBAMHourlyIngestor."""
        super().__init__(session, working_dir)

        # min+max are the BBOX that GAP processes
        # inc and original_min comes from Salient netcdf file
        self.lat_metadata = {
            'min': -12.5969,
            'max': 6.6332,
            'inc': 0.0357437,
            'original_min': -12.5969
        }
        self.lon_metadata = {
            'min': 26.9665,
            'max': 44.0335,
            'inc': 0.0360063,
            'original_min': 26.9665
        }
        self.reindex_tolerance = 0.01
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
        return Dataset.objects.get(name='CBAM Reanalysis Hourly | 2020-2022')

    def _open_dataset(
        self, source_file: DataSourceFile, tmp_file
    ) -> xrDataset:
        """Open temporary NetCDF File.

        :param source_file: Temporary NetCDF File
        :type source_file: DataSourceFile
        :return: xarray dataset
        :rtype: xrDataset
        """
        metadata = source_file.metadata
        s3 = ObjectStorageManager.get_s3_env_vars(
            connection_name=metadata.get(
                's3_connection_name', None
            )
        )
        fs = s3fs.S3FileSystem(
            key=s3.get('S3_ACCESS_KEY_ID'),
            secret=s3.get('S3_SECRET_ACCESS_KEY'),
            client_kwargs=ObjectStorageManager.get_s3_client_kwargs(
                s3=s3
            )
        )

        bucket_name = s3['S3_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/'
        netcdf_url += f'{metadata["directory_path"]}/' + f'{source_file.name}'

        # Open the S3 file in binary read mode
        with fs.open(netcdf_url, 'rb') as s3_file:
            # Copy contents from S3 to temp file
            tmp_file.write(s3_file.read())

        return xr.open_dataset(tmp_file.name, chunks=self.default_chunks)

    def get_empty_shape(self, lat_len, lon_len):
        """Get empty shape for the data.

        :param lat_len: length of latitude
        :type lat_len: int
        :param lon_len: length of longitude
        :type lon_len: int
        :return: empty shape
        :rtype: tuple
        """
        return (
            1,
            self.default_chunks['time'],
            lat_len,
            lon_len
        )

    def get_chunks_for_forecast_date(self, is_single_date=True):
        """Get chunks for forecast date."""
        if not is_single_date:
            return (
                self.default_chunks['date'],
                self.default_chunks['time'],
                self.default_chunks['lat'],
                self.default_chunks['lon']
            )
        return (
            1,
            self.default_chunks['time'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

    def get_data_var_coordinates(self):
        """Get coordinates for data variables."""
        return ['date', 'time', 'lat', 'lon']

    def get_coordinates(
        self, forecast_date: date, new_lat, new_lon, time_idx=None
    ):
        """Get coordinates for the dataset."""
        date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        times = np.array([np.timedelta64(h, 'h') for h in range(24)])
        if time_idx is not None:
            times = times[time_idx:time_idx + 1]
        return {
            'date': ('date', date_array),
            'time': ('time', times),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }

    def get_region_slices(
        self, forecast_date: date, nearest_lat_indices, nearest_lon_indices,
        time_idx: int
    ):
        """Get region slices for update_by_region method."""
        # open existing zarr
        ds = self._open_zarr_dataset()

        # find index of forecast_date
        date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        new_date = date_array[0]
        date_idx = (
            np.where(ds['date'].values == new_date)[0][0]
        )

        ds.close()

        return {
            'date': slice(date_idx, date_idx + 1),
            'time': slice(time_idx, time_idx + 1),
            'lat': slice(
                nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
            'lon': slice(
                nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
        }


    def _append_new_forecast_date(
        self, forecast_date: datetime.date, is_new_dataset=False
    ):
        """Append a new forecast date to the zarr structure.

        The dataset will be initialized with empty values.
        :param forecast_date: forecast date
        :type forecast_date: date
        """
        progress = self._add_progress(
            f'Appending {forecast_date.isoformat()}-{is_new_dataset}'
        )
        start_time = time.time()
        # expand lat and lon
        min_lat = find_start_latlng(self.lat_metadata)
        min_lon = find_start_latlng(self.lon_metadata)
        new_lat = np.arange(
            min_lat, self.lat_metadata['max'] + self.lat_metadata['inc'],
            self.lat_metadata['inc']
        )
        new_lon = np.arange(
            min_lon, self.lon_metadata['max'] + self.lon_metadata['inc'],
            self.lon_metadata['inc']
        )

        # create empty data variables
        empty_shape = self.get_empty_shape(len(new_lat), len(new_lon))
        chunks = self.get_chunks_for_forecast_date()

        # Create the Dataset
        data_vars = {}
        encoding = {
            'date': {
                'chunks': self.default_chunks['date']
            }
        }
        for var in self.variables:
            empty_data = da.full(
                empty_shape, np.nan, dtype='f8', chunks=chunks
            )
            data_vars[var] = (
                self.get_data_var_coordinates(),
                empty_data
            )
            encoding[var] = {
                'chunks': self.get_chunks_for_forecast_date(False)
            }
        ds = xr.Dataset(
            data_vars=data_vars,
            coords=self.get_coordinates(forecast_date, new_lat, new_lon)
        )

        # write/append to zarr
        # note: when writing to a new chunk of forecast_date,
        # the memory usage will be higher than the rest
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        if is_new_dataset:
            # write
            x = ds.to_zarr(
                zarr_url, mode='w', consolidated=True,
                encoding=encoding,
                storage_options=self.s3_options,
                compute=False
            )
        else:
            # append
            x = ds.to_zarr(
                zarr_url, mode='a', append_dim='date',
                consolidated=True,
                storage_options=self.s3_options,
                compute=False
            )
        execute_dask_compute(x)

        # close dataset and remove empty_data
        ds.close()
        del ds
        del empty_data

        # update progress
        total_time = time.time() - start_time
        progress.notes = f"Execution time: {total_time}"
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

    def _update_by_region(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], new_data: dict, time_idx: int):
        """Update new_data to the zarr by its forecast_date.

        The lat_arr and lon_arr should already be chunked
        before calling this method.
        :param forecast_date: forecast date of the new data
        :type forecast_date: date
        :param lat_arr: list of lat coordinate mapping
        :type lat_arr: List[CoordMapping]
        :param lon_arr: list of lon coordinate mapping
        :type lon_arr: List[CoordMapping]
        :param new_data: dictionary of new data
        :type new_data: dict
        """
        # find nearest lat and lon and its indices
        nearest_lat_arr = [lat.nearest_val for lat in lat_arr]
        nearest_lat_indices = [lat.nearest_idx for lat in lat_arr]

        nearest_lon_arr = [lon.nearest_val for lon in lon_arr]
        nearest_lon_indices = [lon.nearest_idx for lon in lon_arr]

        # ensure that the lat/lon indices are in correct order
        assert self._is_sorted_and_incremented(nearest_lat_indices)
        assert self._is_sorted_and_incremented(nearest_lon_indices)

        # Create the dataset with updated data for the region
        data_vars = {
            var: (
                self.get_data_var_coordinates(),
                new_data[var]
            ) for var in new_data
        }
        new_ds = xr.Dataset(
            data_vars=data_vars,
            coords=self.get_coordinates(
                forecast_date,
                nearest_lat_arr,
                nearest_lon_arr,
                time_idx=time_idx
            )
        )

        # write the updated data to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        x = new_ds.to_zarr(
            zarr_url,
            mode='a',
            region=self.get_region_slices(
                forecast_date,
                nearest_lat_indices,
                nearest_lon_indices,
                time_idx
            ),
            storage_options=self.s3_options,
            consolidated=True,
            compute=False
        )
        execute_dask_compute(x)

    def _process_netcdf_file(self, source_file: DataSourceFile):
        """Process the netcdf file."""
        file_date = source_file.start_date_time.date()
        time_idx = source_file.start_date_time.hour
        progress = self._add_progress(
            f'Processing {file_date.isoformat()}--{time_idx}'
        )
        start_time = time.time()

        temp_file = tempfile.NamedTemporaryFile(
            mode='wb', delete=False, suffix='.nc'
        )

        try:
            # open the dataset
            ds = self._open_dataset(source_file, temp_file)

            # Get latitude and longitude sizes
            lat_size = ds.sizes["lat"]
            lon_size = ds.sizes["lon"]

            # create slices for chunks
            lat_chunk_size = self.get_config(
                'lat_chunk_size',
                self.default_chunks['lat']
            )
            lon_chunk_size = self.get_config(
                'lon_chunk_size',
                self.default_chunks['lon']
            )
            var_chunk_size = self.get_config(
                'var_chunk_size',
                10
            )
            lat_slices = self._find_chunk_slices(
                lat_size, lat_chunk_size
            )
            lon_slices = self._find_chunk_slices(
                lon_size, lon_chunk_size
            )
            variable_slices = self._find_chunk_slices(
                len(self.variables), var_chunk_size
            )

            date_array = pd.date_range(
                file_date.isoformat(), periods=1)

            # find index of date
            new_date = date_array[0]

            total_progress = (
                len(lat_slices) * len(lon_slices) * len(variable_slices)
            )
            progress.row_count = total_progress
            progress.notes = f'Processing {total_progress} chunks'
            progress.save()
            total_processed = 0

            for lat_slice in lat_slices:
                for lon_slice in lon_slices:
                    # Extract corresponding latitude & longitude values
                    lat_values = ds.lat.isel(lat=lat_slice).values
                    lon_values = ds.lon.isel(lon=lon_slice).values

                    # transform lat lon arrays
                    lat_arr = self._transform_coordinates_array(
                        lat_values,
                        'lat'
                    )
                    lon_arr = self._transform_coordinates_array(
                        lon_values,
                        'lon'
                    )

                    # get the data array
                    da = ds.isel(lat=lat_slice, lon=lon_slice)

                    # assign date coords
                    da = da.assign_coords(
                        date=date_array[0].value
                    )
                    # expand dimension to date
                    da = da.expand_dims("date")

                    # iterate self.variables with chunks of 10
                    for var_slice in variable_slices:
                        subset_vars = self.variables[var_slice]
                        # write to zarr
                        new_data = {}

                        for var_name in subset_vars:
                            subset_da = da[var_name]
                            new_data[var_name] = subset_da.data

                        # write to zarr
                        self._update_by_region(
                            new_date,
                            lat_arr,
                            lon_arr,
                            new_data,
                            time_idx
                        )

                        total_processed += 1
                        progress.notes = (
                            f'Processing {total_processed}/{total_progress} '
                            'chunks'
                        )
                        progress.save()

            # close the dataset
            ds.close()
        finally:
            # remove the temporary file
            temp_file.close()
            os.unlink(temp_file.name)

        # update progress
        total_time = time.time() - start_time
        progress.notes = f"Execution time: {total_time}"
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

    def _is_date_in_zarr(self, date: datetime.date) -> bool:
        """Check whether a date has been added to zarr file.

        :param date: date to check
        :type date: date
        :return: True if date exists in zarr file.
        :rtype: bool
        """
        if self.created:
            return False
        ds = self._open_zarr_dataset(self.variables)
        # always refetch dates values
        existing_dates = ds[self.DATE_VARIABLE].values
        ds.close()
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in existing_dates

    def _run(self):
        """Process the tio shortterm data into Zarr."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)

        total_files = 0
        files = []
        for source_file in collector.dataset_files.order_by('id').all():
            # check if cancelled
            if self.is_cancelled():
                logger.info('Ingestor CBAM Hourly cancelled!')
                break

            file_date = source_file.start_date_time.date()
            # check if forecast date in zarr
            if not self._is_date_in_zarr(file_date):
                self._append_new_forecast_date(file_date, self.created)

            self._process_netcdf_file(source_file)
            total_files += 1
            files.append(source_file.start_date_time.isoformat())
            if self.created:
                # reset created
                self.created = False
        self.metadata = {
            'total_files': total_files,
            'files': files
        }

    def run(self):
        """Run CBAM Hourly Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error('Ingestor CBAM Hourly failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass
