# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient ingestor.
"""

import os
import uuid
import json
import logging
import datetime
import pytz
import traceback
import s3fs
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da
import salientsdk as sk
import time
from xarray.core.dataset import Dataset as xrDataset
from django.conf import settings
from django.utils import timezone
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from typing import List


from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, CollectorSession, Preferences,
    DatasetAttribute,
    IngestorSessionStatus
)
from gap.ingestor.base import BaseIngestor, BaseZarrIngestor, CoordMapping
from gap.utils.netcdf import NetCDFMediaS3, find_start_latlng
from gap.utils.zarr import BaseZarrReader
from gap.utils.dask import execute_dask_compute


logger = logging.getLogger(__name__)

SALIENT_NUMBER_OF_DAYS = 275  # 9 months


class SalientCollector(BaseIngestor):
    """Collector for Salient seasonal forecast data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize SalientCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')

        # init s3 variables and fs
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('S3_ACCESS_KEY_ID'),
            'secret': self.s3.get('S3_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.fs = s3fs.S3FileSystem(
            key=self.s3.get('S3_ACCESS_KEY_ID'),
            secret=self.s3.get('S3_SECRET_ACCESS_KEY'),
            client_kwargs=BaseZarrReader.get_s3_client_kwargs()
        )

        # reset variables
        self.total_count = 0
        self.data_files = []
        self.metadata = {}

    def _get_date_config(self):
        """Retrieve date from config or default to be today."""
        date_str = '-today'
        if 'forecast_date' in self.session.additional_config:
            date_str = self.session.additional_config['forecast_date']
        return date_str

    def _get_variable_list_config(self):
        """Retrieve variable list."""
        default_vars = [
            "precip",  # precipitation (mm/day)
            "tmin",  # minimum daily temperature (degC)
            "tmax",  # maximum daily temperature (degC)
            "wspd",  # wind speed at 10m (m/s)
            "tsi",  # total solar insolation (W/m^2)
            "rh",  # relative humidity (%),
            "temp"
        ]
        return self.session.additional_config.get(
            'variable_list', default_vars)

    def _get_coords(self):
        """Retrieve polygon coordinates."""
        return self.session.additional_config.get(
            'coords',
            list(Preferences.load().salient_area.coords[0])
        )

    def _convert_forecast_date(self, date_str: str):
        """Convert string forecast date to date object.

        :param date_str: '-today' or 'YYYY-MM-DD'
        :type date_str: str
        :return: date object
        :rtype: Date object
        """
        if date_str == '-today':
            today = datetime.datetime.now()
            return today.date()
        dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return dt.date()

    def _store_as_netcdf_file(self, file_path: str, date_str: str):
        """Store the downscale Salient NetCDF to Default's Storage.

        :param file_path: file path to downscale netcdf
        :type file_path: str
        :param date_str: forecast date in str
        :type date_str: str
        """
        file_stats = os.stat(file_path)
        logger.info(
            f'Salient downscale size: {file_stats.st_size / (1024 * 1024)} MB'
        )
        # prepare start and end dates
        forecast_date = self._convert_forecast_date(date_str)
        end_date = forecast_date + datetime.timedelta(
            days=SALIENT_NUMBER_OF_DAYS
        )
        logger.info(f'Salient dataset from {forecast_date} to {end_date}')

        # store as netcdf to S3
        filename = f'{str(uuid.uuid4())}.nc'
        netcdf_url = (
            NetCDFMediaS3.get_netcdf_base_url(self.s3) +
            'salient_collector/' + filename
        )
        self.fs.put(file_path, netcdf_url)

        # create DataSourceFile
        start_datetime = datetime.datetime(
            forecast_date.year, forecast_date.month, forecast_date.day,
            0, 0, 0, tzinfo=pytz.UTC
        )
        end_datetime = datetime.datetime(
            end_date.year, end_date.month, end_date.day,
            0, 0, 0, tzinfo=pytz.UTC
        )
        self.data_files.append(DataSourceFile.objects.create(
            name=filename,
            dataset=self.dataset,
            start_date_time=start_datetime,
            end_date_time=end_datetime,
            created_on=timezone.now(),
            format=DatasetStore.NETCDF
        ))
        self.total_count += 1
        self.session.dataset_files.set(self.data_files)
        self.metadata = {
            'filepath': netcdf_url,
            'filesize': file_stats.st_size,
            'forecast_date': forecast_date.isoformat(),
            'end_date': end_date.isoformat()
        }

    def _run(self):
        """Download Salient Seasonal Forecast from sdk."""
        logger.info(f'Running data collector for Salient - {self.session.id}.')
        logger.info(f'Working directory: {self.working_dir}')

        # initialize sdk
        sk.set_file_destination(self.working_dir)
        sk.login(
            os.environ.get("SALIENT_SDK_USERNAME"),
            os.environ.get("SALIENT_SDK_PASSWORD")
        )

        # no need to upload new shapefile
        # create the requested locations
        # loc = sk.Location(shapefile=sk.upload_shapefile(
        #     coords=self._get_coords(),
        #     geoname="gap-1",
        #     force=False)
        # )
        # use existing gap-1.geojson
        # note: i'm unable to upload new shapefile, but gap-1 exists
        loc = sk.Location(shapefile='gap-1.geojson')

        # request data
        fcst_file = sk.downscale(
            loc=loc,
            variables=self._get_variable_list_config(),
            date=self._get_date_config(),
            members=50,
            force=False,
            verbose=settings.DEBUG,
            length=SALIENT_NUMBER_OF_DAYS
        )

        self._store_as_netcdf_file(fcst_file, self._get_date_config())

        # remove the temporary file
        if os.path.exists(fcst_file):
            os.remove(fcst_file)

    def run(self):
        """Run Salient Data Collector."""
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error('Collector Salient failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass


class SalientIngestor(BaseZarrIngestor):
    """Ingestor for Salient seasonal forecast data."""

    default_chunks = {
        'ensemble': 50,
        'forecast_day': 20,
        'lat': 20,
        'lon': 20
    }
    forecast_date_chunk = 10
    num_dates = SALIENT_NUMBER_OF_DAYS
    DATE_VARIABLE = 'forecast_date'

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize SalientIngestor."""
        super().__init__(session, working_dir)

        # min+max are the BBOX that GAP processes
        # inc and original_min comes from Salient netcdf file
        self.lat_metadata = {
            'min': -27,
            'max': 16,
            'inc': 0.25,
            'original_min': -0.625
        }
        self.lon_metadata = {
            'min': 21.8,
            'max': 52,
            'inc': 0.25,
            'original_min': 33.38
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
        return Dataset.objects.get(name='Salient Seasonal Forecast')

    def _get_s3_filepath(self, source_file: DataSourceFile):
        """Get Salient NetCDF temporary file from Collector.

        :param source_file: Temporary NetCDF File
        :type source_file: DataSourceFile
        :return: s3 path to the file
        :rtype: str
        """
        dir_prefix = os.environ.get('GAP_S3_PRODUCTS_DIR_PREFIX', '')
        return os.path.join(
            dir_prefix,
            'salient_collector',
            source_file.name
        )

    def _remove_temporary_source_file(
            self, source_file: DataSourceFile, file_path: str):
        """Remove temporary file from collector.

        :param source_file: Temporary File
        :type source_file: DataSourceFile
        :param file_path: s3 file path
        :type file_path: str
        """
        try:
            s3_storage: S3Boto3Storage = storages["gap_products"]
            s3_storage.delete(file_path)
        except Exception as ex:
            logger.error(
                f'Failed to remove original source_file {file_path}!', ex)
        finally:
            source_file.delete()

    def _open_dataset(self, source_file: DataSourceFile) -> xrDataset:
        """Open temporary NetCDF File.

        :param source_file: Temporary NetCDF File
        :type source_file: DataSourceFile
        :return: xarray dataset
        :rtype: xrDataset
        """
        s3 = BaseZarrReader.get_s3_variables()
        fs = s3fs.S3FileSystem(
            key=s3.get('S3_ACCESS_KEY_ID'),
            secret=s3.get('S3_SECRET_ACCESS_KEY'),
            client_kwargs=NetCDFMediaS3.get_s3_client_kwargs()
        )

        prefix = s3['S3_DIR_PREFIX']
        bucket_name = s3['S3_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/{prefix}'
        if not netcdf_url.endswith('/'):
            netcdf_url += '/'
        netcdf_url += 'salient_collector/' + f'{source_file.name}'
        return xr.open_dataset(
            fs.open(netcdf_url), chunks=self.default_chunks)

    def _check_netcdf_file_exists(self, file_path: str) -> bool:
        """Check if the NetCDF file exists in S3.

        :param file_path: path to the NetCDF file
        :type file_path: str
        :return: True if exists, False otherwise
        :rtype: bool
        """
        s3_storage: S3Boto3Storage = storages["gap_products"]
        return s3_storage.exists(file_path)

    def _run(self):
        """Run Salient ingestor."""
        logger.info(f'Running data ingestor for Salient: {self.session.id}.')
        total_files = 0
        forecast_dates = []
        for collector in self.session.collectors.order_by('id'):
            # Query the datasource file
            source_file = (
                collector.dataset_files.first()
            )
            if source_file is None:
                continue

            file_path = self._get_s3_filepath(source_file)
            if not self._check_netcdf_file_exists(file_path):
                logger.warning(f'DataSource {file_path} does not exist!')
                continue

            # convert to zarr
            forecast_date = source_file.start_date_time.date()

            # check if forecast date in zarr
            if not self._is_date_in_zarr(forecast_date):
                self._append_new_forecast_date(forecast_date, self.created)

            self._process_netcdf_file(source_file, forecast_date)

            # update start/end date of zarr datasource file
            self._update_zarr_source_file(forecast_date)

            # delete netcdf datasource file
            remove_temp_file = self.get_config('remove_temp_file', True)
            if remove_temp_file:
                self._remove_temporary_source_file(source_file, file_path)

            total_files += 1
            forecast_dates.append(forecast_date.isoformat())
            if self.created:
                # reset created
                self.created = False
        self.metadata = {
            'total_files': total_files
        }

        # invalidate zarr cache
        self._invalidate_zarr_cache()

    def _process_netcdf_file(
        self, source_file: DataSourceFile, forecast_date: datetime.date
    ):
        """Process the netcdf file."""
        progress = self._add_progress(
            f'Processing {forecast_date.isoformat()}'
        )
        start_time = time.time()

        # open the dataset
        ds = self._open_dataset(source_file)

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

        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)

        # open existing zarr
        existing_ds = self._open_zarr_dataset(
            drop_variables=self.variables
        )

        # find index of forecast_date
        new_forecast_date = forecast_date_array[0]
        forecast_date_idx = (
            np.where(
                existing_ds['forecast_date'].values == new_forecast_date
            )[0][0]
        )
        existing_ds.close()

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
                lat_arr = self._transform_coordinates_array(lat_values, 'lat')
                lon_arr = self._transform_coordinates_array(lon_values, 'lon')

                # get the data array
                da = ds.isel(lat=lat_slice, lon=lon_slice)

                # assign forecast_date coords
                da = da.assign_coords(
                    forecast_date=forecast_date_array[0].value
                )

                # transform forecast_day into number of days
                fd = np.datetime64(forecast_date.isoformat())
                forecast_day_idx = (da['forecast_day'] - fd).dt.days.data
                da = da.assign_coords(
                    forecast_day_idx=("forecast_day", forecast_day_idx))
                da = da.swap_dims({'forecast_day': 'forecast_day_idx'})
                da = da.drop_vars('forecast_day')

                # expand dimension to forecast_date
                da = da.expand_dims("forecast_date")

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
                        new_forecast_date, forecast_date_idx,
                        lat_arr, lon_arr, subset_vars, new_data
                    )

                    total_processed += 1
                    progress.notes = (
                        f'Processing {total_processed}/{total_progress} '
                        'chunks'
                    )
                    progress.save()

        # close the dataset
        ds.close()

        # update progress
        total_time = time.time() - start_time
        progress.notes = f"Execution time: {total_time}"
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

    def _append_new_forecast_date(
        self, forecast_date: datetime.date, is_new_dataset=False,
        init_number_of_months=None
    ):
        """Append a new forecast date to the zarr structure.

        The dataset will be initialized with empty values.
        :param forecast_date: forecast date
        :type forecast_date: date
        """
        progress = self._add_progress(
            f'Appending {forecast_date.isoformat()}-{init_number_of_months}'
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

        # create forecast date array
        if init_number_of_months:
            forecast_date_array = pd.date_range(
                start=forecast_date.isoformat(),
                periods=init_number_of_months,
                freq='MS'
            )
        else:
            forecast_date_array = pd.date_range(
                forecast_date.isoformat(),
                periods=1
            )
        forecast_day_indices = np.arange(0, self.num_dates, 1)

        # set chunks for each data var
        ensemble_chunks = (
            self.forecast_date_chunk,
            self.default_chunks['ensemble'],
            self.default_chunks['forecast_day'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )
        non_ensemble_chunks = (
            self.forecast_date_chunk,
            self.default_chunks['forecast_day'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )
        data_vars = {}
        encoding = {
            'forecast_date': {
                'chunks': self.forecast_date_chunk
            }
        }
        for var_name in self.variables:
            if var_name.endswith('_clim'):
                empty_shape = (
                    1 if init_number_of_months is None else
                    init_number_of_months,
                    len(forecast_day_indices),
                    len(new_lat),
                    len(new_lon)
                )
                empty_data = da.full(
                    empty_shape, np.nan,
                    dtype='f8',
                    chunks=non_ensemble_chunks
                )
                data_vars[var_name] = (
                    ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
                    empty_data
                )
            else:
                empty_shape = (
                    1 if init_number_of_months is None else
                    init_number_of_months,
                    self.default_chunks['ensemble'],
                    len(forecast_day_indices),
                    len(new_lat),
                    len(new_lon)
                )
                empty_data = da.full(
                    empty_shape, np.nan, dtype='f8', chunks=ensemble_chunks
                )
                data_vars[var_name] = (
                    [
                        'forecast_date',
                        'ensemble',
                        'forecast_day_idx',
                        'lat',
                        'lon'
                    ],
                    empty_data
                )
            encoding[var_name] = {
                'chunks': (
                    non_ensemble_chunks if var_name.endswith('_clim') else
                    ensemble_chunks
                )
            }

        # Create the Dataset
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'forecast_date': ('forecast_date', forecast_date_array),
                'ensemble': ('ensemble', np.arange(50)),
                'forecast_day_idx': (
                    'forecast_day_idx', forecast_day_indices),
                'lat': ('lat', new_lat),
                'lon': ('lon', new_lon)
            }
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
                zarr_url, mode='a', append_dim='forecast_date',
                consolidated=True,
                storage_options=self.s3_options,
                compute=False
            )

        if is_new_dataset and init_number_of_months:
            # only generate the metadata
            pass
        else:
            # execute_dask_compute will write empty data
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
        self, forecast_date: pd.Timestamp, forecast_date_idx,
        lat_arr: List[CoordMapping], lon_arr: List[CoordMapping],
        subset_vars: List[str], new_data: dict
    ):
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
        forecast_day_indices = np.arange(0, self.num_dates, 1)
        # find nearest lat and lon and its indices
        nearest_lat_arr = [lat.nearest_val for lat in lat_arr]
        nearest_lat_indices = [lat.nearest_idx for lat in lat_arr]

        nearest_lon_arr = [lon.nearest_val for lon in lon_arr]
        nearest_lon_indices = [lon.nearest_idx for lon in lon_arr]

        # ensure that the lat/lon indices are in correct order
        assert self._is_sorted_and_incremented(nearest_lat_indices)
        assert self._is_sorted_and_incremented(nearest_lon_indices)

        # Init data variables
        data_vars = {}
        has_ensemble = False
        for var in subset_vars:
            if var.endswith('_clim'):
                data_vars[var] = (
                    ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
                    new_data[var]
                )
            else:
                data_vars[var] = (
                    [
                        'forecast_date',
                        'ensemble',
                        'forecast_day_idx',
                        'lat',
                        'lon'
                    ],
                    new_data[var]
                )
                has_ensemble = True
        # Create the new dataset
        new_ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'forecast_date': [forecast_date],
                'forecast_day_idx': forecast_day_indices,
                'lat': nearest_lat_arr,
                'lon': nearest_lon_arr
            }
        )

        # write the updated data to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        regions = {
            'forecast_date': slice(
                forecast_date_idx, forecast_date_idx + 1),
            'ensemble': slice(None),
            'forecast_day_idx': slice(None),
            'lat': slice(
                nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
            'lon': slice(
                nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
        }
        if not has_ensemble:
            # remove ensemble from the region
            regions.pop('ensemble')
        x = new_ds.to_zarr(
            zarr_url,
            mode='a',
            region=regions,
            storage_options=self.s3_options,
            consolidated=True,
            compute=False
        )
        execute_dask_compute(x)

    def run(self):
        """Run Salient Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error('Ingestor Salient failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass
