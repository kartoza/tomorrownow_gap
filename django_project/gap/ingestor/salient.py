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
import salientsdk as sk
from xarray.core.dataset import Dataset as xrDataset
from django.conf import settings
from django.utils import timezone
from django.core.files.storage import default_storage


from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, CollectorSession, Preferences
)
from gap.ingestor.base import BaseIngestor, BaseZarrIngestor
from gap.utils.netcdf import NetCDFMediaS3, find_start_latlng
from gap.utils.zarr import BaseZarrReader
from gap.utils.dask import execute_dask_compute


logger = logging.getLogger(__name__)


class SalientCollector(BaseIngestor):
    """Collector for Salient seasonal forecast data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize SalientCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')

        # init s3 variables and fs
        self.s3 = NetCDFMediaS3.get_s3_variables('salient')
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': NetCDFMediaS3.get_s3_client_kwargs()
        }
        self.fs = s3fs.S3FileSystem(
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=NetCDFMediaS3.get_s3_client_kwargs()
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
        end_date = forecast_date + datetime.timedelta(days=275)
        logger.info(f'Salient dataset from {forecast_date} to {end_date}')

        # store as netcdf to S3
        filename = f'{str(uuid.uuid4())}.nc'
        netcdf_url = (
            NetCDFMediaS3.get_netcdf_base_url(self.s3) + filename
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
            length=275  # 9 months
        )

        self._store_as_netcdf_file(fcst_file, self._get_date_config())

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
        dir_prefix = os.environ.get('MINIO_AWS_DIR_PREFIX', '')
        return os.path.join(
            dir_prefix,
            'salient',
            source_file.name
        )

    def _open_dataset(self, source_file: DataSourceFile) -> xrDataset:
        """Open temporary NetCDF File.

        :param source_file: Temporary NetCDF File
        :type source_file: DataSourceFile
        :return: xarray dataset
        :rtype: xrDataset
        """
        s3 = NetCDFMediaS3.get_s3_variables('salient')
        fs = s3fs.S3FileSystem(
            key=s3.get('AWS_ACCESS_KEY_ID'),
            secret=s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=NetCDFMediaS3.get_s3_client_kwargs()
        )

        prefix = s3['AWS_DIR_PREFIX']
        bucket_name = s3['AWS_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/{prefix}'
        if not netcdf_url.endswith('/'):
            netcdf_url += '/'
        netcdf_url += f'{source_file.name}'
        return xr.open_dataset(
            fs.open(netcdf_url), chunks=self.default_chunks)

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
            s3_storage = default_storage
            file_path = self._get_s3_filepath(source_file)
            if not s3_storage.exists(file_path):
                logger.warning(f'DataSource {file_path} does not exist!')
                continue

            # open the dataset
            dataset = self._open_dataset(source_file)

            # convert to zarr
            forecast_date = source_file.start_date_time.date()
            self.store_as_zarr(dataset, forecast_date)

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
            'total_files': total_files,
            'forecast_dates': forecast_dates
        }

        # invalidate zarr cache
        self._invalidate_zarr_cache()

    def store_as_zarr(self, dataset: xrDataset, forecast_date: datetime.date):
        """Store xarray dataset from forecast_date into Salient zarr file.

        The dataset will be expanded by lat and lon.
        :param dataset: xarray dataset from NetCDF file
        :type dataset: xrDataset
        :param forecast_date: forecast date
        :type forecast_date: datetime.date
        """
        forecast_dates = pd.date_range(
            f'{forecast_date.isoformat()}', periods=1)
        data_vars = {}
        for var_name, da in dataset.data_vars.items():
            if var_name.endswith('_clim'):
                data_vars[var_name] = da.expand_dims(
                    'forecast_date',
                    axis=0
                ).chunk({
                    'forecast_day': self.default_chunks['forecast_day'],
                    'lat': self.default_chunks['lat'],
                    'lon': self.default_chunks['lon']
                })
            else:
                data_vars[var_name] = da.expand_dims(
                    'forecast_date',
                    axis=0
                ).chunk(self.default_chunks)

        # Create the dataset
        zarr_ds = xr.Dataset(
            data_vars,
            coords={
                'forecast_date': ('forecast_date', forecast_dates),
                'forecast_day': (
                    'forecast_day', dataset.coords['forecast_day'].values
                ),
                'lat': ('lat', dataset.coords['lat'].values),
                'lon': ('lon', dataset.coords['lon'].values),
                'ensemble': ('ensemble', np.arange(50)),
            }
        )

        # transform forecast_day into number of days
        fd = np.datetime64(forecast_date.isoformat())
        forecast_day_idx = (zarr_ds['forecast_day'] - fd).dt.days.data
        zarr_ds = zarr_ds.assign_coords(
            forecast_day_idx=("forecast_day", forecast_day_idx))
        zarr_ds = zarr_ds.swap_dims({'forecast_day': 'forecast_day_idx'})
        zarr_ds = zarr_ds.drop_vars('forecast_day')

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
        expanded_ds = zarr_ds.reindex(
            lat=new_lat, lon=new_lon, method='nearest',
            tolerance=self.reindex_tolerance
        )

        # rechunk the dataset
        expanded_ds = expanded_ds.chunk({
            'forecast_date': self.forecast_date_chunk,
            'ensemble': self.default_chunks['ensemble'],
            'forecast_day_idx': self.default_chunks['forecast_day'],
            'lat': self.default_chunks['lat'],
            'lon': self.default_chunks['lon']
        })

        # write to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )

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
        encoding = {
            'forecast_date': {
                'chunks': self.forecast_date_chunk
            }
        }
        var_name: str
        for var_name, da in expanded_ds.data_vars.items():
            encoding[var_name] = {
                'chunks': (
                    non_ensemble_chunks if var_name.endswith('_clim') else
                    ensemble_chunks
                )
            }

        # update the zarr file
        if self.created:
            x = expanded_ds.to_zarr(
                zarr_url, mode='w', consolidated=True,
                storage_options=self.s3_options,
                encoding=encoding,
                compute=False
            )
        else:
            x = expanded_ds.to_zarr(
                zarr_url, mode='a-', append_dim='forecast_date',
                consolidated=True,
                storage_options=self.s3_options,
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
