# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Bias Adjust ingestor.
"""

import os
import json
import logging
import pytz
import s3fs
import traceback
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da
import time
import calendar
import datetime
from django.utils import timezone
from typing import List
from xarray.core.dataset import Dataset as xrDataset


from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus,
    CollectorSession
)
from gap.utils.netcdf import find_start_latlng
from gap.utils.zarr import BaseZarrReader
from gap.ingestor.base import BaseIngestor, BaseZarrIngestor, CoordMapping
from gap.utils.dask import execute_dask_compute


logger = logging.getLogger(__name__)


class CBAMBiasAdjustCollector(BaseIngestor):
    """Data collector for CBAM Historical data (Bias-Adjust)."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize CBAMCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(
            name='CBAM Climate Reanalysis (Bias-Corrected)')
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.fs = s3fs.S3FileSystem(
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=BaseZarrReader.get_s3_client_kwargs()
        )

        self.dir_path = session.additional_config.get('directory_path', None)
        if not self.dir_path:
            raise RuntimeError('Collector session must have directory_path!')
        self.total_count = 0
        self.data_files = []

    def _parse_filename(self, file_path):
        """Parse NETCDF filename."""
        attribute = os.path.split(file_path)[0]
        netcdf_filename = os.path.split(file_path)[1]
        netcdf_filename_no_ext = netcdf_filename.split('.')[0]
        cleaned_name = netcdf_filename_no_ext.replace(
            f'{attribute}_', '')

        file_date = datetime.datetime.strptime(
            cleaned_name.split('_')[1], '%Y')

        return attribute, file_date

    def _run(self):
        """Collect list of files in the CBAM S3 directory.

        The bias adjusted is currently hosted on TNGAP Products.
        The NetCDF Files are for a year and just contain 1 attribute.
        The file structure must be:
            '{attribute}/{attribute}_interpolated_YYYY_RBF.nc'
        """
        logger.info(f'Check NETCDF Files by dataset {self.dataset.name}')

        bucket_name = self.s3.get('AWS_BUCKET_NAME')
        self.total_count = 0
        for dirpath, dirnames, filenames in \
            self.fs.walk(f's3://{bucket_name}/{self.dir_path}'):
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

                # get the file_path
                cleaned_dir = dirpath.replace(
                    f'{bucket_name}/{self.dir_path}', '')
                if cleaned_dir:
                    file_path = (
                        f'{cleaned_dir}{filename}' if
                        cleaned_dir.endswith('/') else
                        f'{cleaned_dir}/{filename}'
                    )
                else:
                    file_path = filename
                if file_path.startswith('/'):
                    file_path = file_path[1:]

                # check existing and skip if exist
                check_exist = DataSourceFile.objects.filter(
                    name=file_path,
                    dataset=self.dataset,
                    format=DatasetStore.NETCDF
                ).exists()
                if check_exist:
                    continue

                # parse datetime from filename
                attribute, file_date = self._parse_filename(file_path)
                start_datetime = datetime.datetime(
                    file_date.year, 1, 1,
                    0, 0, 0, tzinfo=pytz.UTC
                )
                end_datetime = datetime.datetime(
                    file_date.year, 12, 31,
                    0, 0, 0, tzinfo=pytz.UTC
                )

                # insert record to DataSourceFile
                self.data_files.append(DataSourceFile.objects.create(
                    name=file_path,
                    dataset=self.dataset,
                    start_date_time=start_datetime,
                    end_date_time=end_datetime,
                    created_on=timezone.now(),
                    format=DatasetStore.NETCDF,
                    metadata={
                        'attribute': attribute,
                        'directory_path': self.dir_path
                    }
                ))
                self.total_count += 1

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


class CBAMBiasAdjustIngestor(BaseZarrIngestor):
    """Ingestor CBAM Bias Adjust data into Zarr."""

    default_chunks = {
        'date': 90,
        'lat': 100,
        'lon': 100
    }

    variables = [
        'total_rainfall',
        'max_total_temperature',
        'min_total_temperature',
        'total_solar_irradiance'
    ]

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize CBAMBiasAdjustIngestor."""
        super().__init__(session, working_dir)

        self.metadata = {
            'total_processed': 0
        }

        # min+max are the BBOX that GAP processes
        self.lat_metadata = {
            'min': -27,
            'max': 16,
            'inc': 0.03574368,
            'original_min': -12.5969
        }
        self.lon_metadata = {
            'min': 21.8,
            'max': 52,
            'inc': 0.036006329,
            'original_min': 26.9665
        }
        self.reindex_tolerance = 0.001
        self.existing_dates = None

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='CBAM Climate Reanalysis (Bias-Corrected)',
            store_type=DatasetStore.ZARR
        )

    def _days_in_year(self, year):
        """Return number of days in year."""
        return 365 + calendar.isleap(year)

    def _append_new_date(
            self, date: datetime.date, is_new_dataset=False):
        """Append a new date to the zarr structure.

        The dataset will be initialized with empty values.
        :param date: historical date
        :type date: date
        """
        logger.info(f'Appending new dates in year {date.year}')
        total_days_in_year = self._days_in_year(date.year)
        start_date = datetime.date(date.year, 1, 1)
        end_date = datetime.date(date.year, 12, 31)

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
        empty_shape = (
            total_days_in_year,
            len(new_lat),
            len(new_lon)
        )
        chunks = (
            self.default_chunks['date'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

        # Create the Dataset
        date_array = pd.date_range(
            start=start_date.isoformat(), end=end_date.isoformat())
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
                ['date', 'lat', 'lon'],
                empty_data
            )
            encoding[var] = {
                'chunks': (
                    self.default_chunks['date'],
                    self.default_chunks['lat'],
                    self.default_chunks['lon']
                )
            }
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'date': ('date', date_array),
                'lat': ('lat', new_lat),
                'lon': ('lon', new_lon)
            }
        )

        # write/append to zarr
        # note: when writing to a new chunk of date,
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

    def _open_netcdf_file(self, source_file: DataSourceFile) -> xrDataset:
        """Open netcdf file from DataSourceFile.

        DataSourceFile must have metadata of:
        - attribute
        - directory_path
        :param source_file: source file of NetCDF CBAM Bias adjusted
        :type source_file: DataSourceFile
        :raises RuntimeError: raises when no attribute/directory_path
            in metadata
        :return: xarray dataset
        :rtype: xrDataset
        """
        fs = s3fs.S3FileSystem(
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=BaseZarrReader.get_s3_client_kwargs()
        )

        # check for directory_path
        prefix = source_file.metadata.get('directory_path', None)
        if not prefix:
            raise RuntimeError('DataSourceFile must have directory_path!')
        # check for attribute
        attribute = source_file.metadata.get('attribute', None)
        if not attribute:
            raise RuntimeError('DataSourceFile must have attribute!')

        # build url
        bucket_name = self.s3['AWS_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/{prefix}'
        if not netcdf_url.endswith('/'):
            netcdf_url += '/'
        netcdf_url += f'{source_file.name}'

        # open dataset using default chunk
        ds = xr.open_dataset(
            fs.open(netcdf_url), chunks='auto')

        # rename time attribute to date
        ds = ds.rename({'time': 'date'})
        # rechunk
        ds = ds.chunk(chunks=self.default_chunks)

        return ds

    def _write_by_region(
            self, date_arr: List[CoordMapping], lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], new_data: dict):
        """Write new_data to existing zarr file.

        :param date_arr: Array of date
        :type date_arr: List[CoordMapping]
        :param lat_arr: Array of lat
        :type lat_arr: List[CoordMapping]
        :param lon_arr: Array of lon
        :type lon_arr: List[CoordMapping]
        :param new_data: Dictionary of attribute and DataArray
        :type new_data: dict
        """
        # find index of date
        date_indices = [dt.nearest_idx for dt in date_arr]
        date_arr_values = [dt.value for dt in date_arr]

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
                ['date', 'lat', 'lon'],
                new_data[var].data
            ) for var in new_data
        }
        new_ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'date': date_arr_values,
                'lat': nearest_lat_arr,
                'lon': nearest_lon_arr
            }
        )

        # write the updated data to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        x = new_ds.to_zarr(
            zarr_url,
            mode='a',
            region={
                'date': slice(
                    date_indices[0], date_indices[-1] + 1),
                'lat': slice(
                    nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
                'lon': slice(
                    nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
            },
            storage_options=self.s3_options,
            consolidated=True,
            compute=False
        )
        execute_dask_compute(x)

        new_ds.close()
        del data_vars

    def _run(self):
        """Process CBAM NetCDF Files into GAP Zarr file."""
        self.metadata = {
            'start_date': None,
            'end_date': None,
            'total_processed': 0
        }

        # query NetCDF DataSourceFile for CBAM Dataset
        sources = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.NETCDF
        ).order_by('start_date_time')

        logger.info(f'Total CBAM source files: {sources.count()}')
        if not sources.exists():
            return

        # iterate for each NetCDF DataFileSource
        for source in sources:
            # check if cancelled
            if self.is_cancelled():
                break

            start_time = time.time()

            # open netcdffile
            ds = self._open_netcdf_file(source)
            attribute = source.metadata.get('attribute')

            # add progress object
            IngestorSessionProgress.objects.update_or_create(
                session=self.session,
                filename=source.name,
                defaults={
                    'row_count': 0,
                    'status': IngestorSessionStatus.RUNNING,
                    'notes': ''
                }
            )

            # check if year is added to the dataset
            if not self._is_date_in_zarr(source.start_date_time.date()):
                self._append_new_date(
                    source.start_date_time.date(), self.created)
                self.created = False
                self.existing_dates = None

            # transform date lat lon arrays
            lat_arr = self._transform_coordinates_array(
                ds.lat.values, 'lat')
            lon_arr = self._transform_coordinates_array(
                ds.lon.values, 'lon')
            date_arr = self._transform_coordinates_array(
                ds.date.values, 'date')

            # split date lat lon into slices
            date_slices = self._find_chunk_slices(
                len(date_arr), self.default_chunks['date'])
            lat_slices = self._find_chunk_slices(
                len(lat_arr), self.default_chunks['lat'])
            lon_slices = self._find_chunk_slices(
                len(lon_arr), self.default_chunks['lon'])

            for date_slice in date_slices:
                for lat_slice in lat_slices:
                    for lon_slice in lon_slices:
                        # get each chunk
                        date_chunks = date_arr[date_slice]
                        lat_chunks = lat_arr[lat_slice]
                        lon_chunks = lon_arr[lon_slice]

                        # get the data by positional index
                        new_data = {
                            attribute: (
                                ds[attribute].isel(
                                    date=date_slice, lat=lat_slice,
                                    lon=lon_slice
                                )
                            )
                        }

                        # write to zarr
                        self._write_by_region(
                            date_chunks, lat_chunks, lon_chunks, new_data)

            # update progress
            self.metadata['total_processed'] += 1
            IngestorSessionProgress.objects.update_or_create(
                session=self.session,
                filename=source.name,
                defaults={
                    'row_count': 0,
                    'status': IngestorSessionStatus.SUCCESS,
                    'notes': f'Total time: {time.time() - start_time} s'
                }
            )
            if (
                self.metadata['start_date'] is None or
                self.metadata['start_date'] > source.start_date_time.date()
            ):
                self.metadata['start_date'] = source.start_date_time.date()

            if (
                self.metadata['end_date'] is None or
                self.metadata['end_date'] < source.end_date_time.date()
            ):
                self.metadata['end_date'] = source.end_date_time.date()

            # close netcdf ds
            ds.close()

    def run(self):
        """Run CBAM Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
            logger.info(f'Ingestor CBAM NetCDFFile: {self.session.notes}')

            # update datasourcefile
            if (
                self.metadata['start_date'] and self.metadata['start_date'] <
                self.datasource_file.start_date_time.date()
            ):
                self.datasource_file.start_date_time = (
                    datetime.datetime.combine(
                        self.metadata['start_date'], datetime.time.min,
                        tzinfo=pytz.UTC
                    )
                )
            if (
                self.metadata['end_date'] and self.metadata['end_date'] >
                self.datasource_file.end_date_time.date()
            ):
                self.datasource_file.end_date_time = (
                    datetime.datetime.combine(
                        self.metadata['end_date'], datetime.time.min,
                        tzinfo=pytz.UTC
                    )
                )
            self.datasource_file.save()
        except Exception as e:
            logger.error('Ingestor CBAM failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass
