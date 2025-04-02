# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM ingestor.
"""

import os
import json
import logging
import datetime
import pytz
import s3fs
import traceback
import numpy as np
import pandas as pd
from xarray.core.dataset import Dataset as xrDataset
from django.utils import timezone


from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus,
    CollectorSession
)
from gap.providers import CBAMNetCDFReader
from gap.utils.netcdf import NetCDFProvider, find_start_latlng
from gap.utils.zarr import BaseZarrReader
from gap.ingestor.base import BaseIngestor
from gap.utils.dask import execute_dask_compute


logger = logging.getLogger(__name__)


class CBAMCollector(BaseIngestor):
    """Data collector for CBAM Historical data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize CBAMCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        self.s3 = NetCDFProvider.get_s3_variables(self.dataset.provider)
        self.fs = s3fs.S3FileSystem(
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=NetCDFProvider.get_s3_client_kwargs(
                self.dataset.provider)
        )
        self.total_count = 0
        self.data_files = []

    def _run(self):
        """Collect list of files in the CBAM S3 directory.

        The filename must be: 'YYYY-MM-DD.nc'
        """
        logger.info(f'Check NETCDF Files by dataset {self.dataset.name}')
        directory_path = self.s3.get('AWS_DIR_PREFIX')
        bucket_name = self.s3.get('AWS_BUCKET_NAME')
        self.total_count = 0
        for dirpath, dirnames, filenames in \
            self.fs.walk(f's3://{bucket_name}/{directory_path}'):
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
                    f'{bucket_name}/{directory_path}', '')
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
                netcdf_filename = os.path.split(file_path)[1]
                file_date = datetime.datetime.strptime(
                    netcdf_filename.split('.')[0], '%Y-%m-%d')
                start_datetime = datetime.datetime(
                    file_date.year, file_date.month, file_date.day,
                    0, 0, 0, tzinfo=pytz.UTC
                )

                # insert record to DataSourceFile
                self.data_files.append(DataSourceFile.objects.create(
                    name=file_path,
                    dataset=self.dataset,
                    start_date_time=start_datetime,
                    end_date_time=start_datetime,
                    created_on=timezone.now(),
                    format=DatasetStore.NETCDF
                ))
                self.total_count += 1

        if self.total_count > 0:
            logger.info(
                f'{self.dataset.name} - Added new NetCDFFile: '
                f'{self.total_count}'
            )
            self.session.dataset_files.set(self.data_files)

    def run(self):
        """Run CBAM Data Collector."""
        try:
            self._run()
        except Exception as e:
            logger.error('Collector CBAM failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass


class CBAMIngestor(BaseIngestor):
    """Ingestor for CBAM Historical Data."""

    DEFAULT_FORMAT = DatasetStore.ZARR

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize CBAMIngestor."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }

        # get zarr data source file
        self.datasource_file, self.created = self._init_datasource()
        if self.created:
            self.datasource_file.start_date_time = timezone.now()
            self.datasource_file.end_date_time = (
                timezone.now() - datetime.timedelta(days=20 * 365)
            )
            self.datasource_file.save()

        # min+max are the BBOX that GAP processes
        # inc and original_min comes from CBAM netcdf file
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

        :raises NotImplementedError: should be implemented in child class
        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(name='CBAM Climate Reanalysis')

    def is_date_in_zarr(self, date: datetime.date) -> bool:
        """Check whether a date has been added to zarr file.

        :param date: date to check
        :type date: datetime.date
        :return: True if date exists in zarr file.
        :rtype: bool
        """
        if self.created:
            return False
        if self.existing_dates is None:
            reader = BaseZarrReader(self.dataset, [], None, None, None)
            reader.setup_reader()
            ds = reader.open_dataset(self.datasource_file)
            self.existing_dates = ds.date.values
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in self.existing_dates

    def store_as_zarr(self, dataset: xrDataset, date: datetime.date):
        """Store dataset from NetCDF into CBAM Zarr file.

        if zarr doesn't exist, 'w' mode will be used, otherwise
        it's going to use 'a' mode with append_dim date.

        :param dataset: Dataset to be added
        :type dataset: xrDataset
        :param date: Date of a dataset
        :type date: datetime.date
        """
        new_date = pd.date_range(f'{date.isoformat()}', periods=1)
        dataset = dataset.assign_coords(date=new_date)
        if 'Date' in dataset.attrs:
            del dataset.attrs['Date']

        # Generate the new latitude and longitude arrays
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
        expanded_ds = dataset.reindex(
            lat=new_lat, lon=new_lon, method='nearest',
            tolerance=self.reindex_tolerance
        )

        # generate the zarr_url
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )

        # create chunks for data variables
        date_chunksize = 3 * 30  # 3 months
        encoding = {
            'date': {
                'units': f'days since {date.isoformat()}',
                'chunks': date_chunksize
            }
        }
        chunks = (date_chunksize, 300, 300)
        for var_name, da in expanded_ds.data_vars.items():
            encoding[var_name] = {
                'chunks': chunks
            }

        # store to zarr
        if self.created:
            self.created = False
            x = expanded_ds.to_zarr(
                zarr_url, mode='w', consolidated=True,
                storage_options=self.s3_options, encoding=encoding,
                compute=False
            )
        else:
            x = expanded_ds.to_zarr(
                zarr_url, mode='a-', append_dim='date', consolidated=True,
                storage_options=self.s3_options, compute=False
            )
        execute_dask_compute(x)

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

        # Initialize NetCDFReader
        source_reader = CBAMNetCDFReader(self.dataset, [], None, None, None)
        source_reader.setup_reader()
        total_monthyear = 0
        progress = None
        curr_monthyear = None

        # iterate for each NetCDF DataFileSource
        for source in sources:
            # check if cancelled
            if self.is_cancelled():
                break

            iter_monthyear = source.start_date_time.date()
            # check if iter_monthyear is already in dataset
            if self.is_date_in_zarr(iter_monthyear):
                continue

            # initialize progress if empty
            if curr_monthyear is None:
                self.metadata['start_date'] = iter_monthyear
                curr_monthyear = iter_monthyear
                progress = IngestorSessionProgress.objects.create(
                    session=self.session,
                    filename=f'{curr_monthyear.year}-{curr_monthyear.month}',
                    row_count=0
                )

            # merge source_ds to target zarr
            source_ds = source_reader.open_dataset(source)
            self.store_as_zarr(source_ds, iter_monthyear)

            # update progress
            self.metadata['total_processed'] += 1
            total_monthyear += 1
            if (
                curr_monthyear.year != iter_monthyear.year
            ):
                # update ingestion progress
                if progress:
                    progress.row_count = total_monthyear
                    progress.status = IngestorSessionStatus.SUCCESS
                    progress.save()
                # reset vars
                total_monthyear = 0
                curr_monthyear = iter_monthyear
                progress = IngestorSessionProgress.objects.create(
                    session=self.session,
                    filename=f'{curr_monthyear.year}-{curr_monthyear.month}',
                    row_count=0
                )

        # save last progress
        if progress:
            progress.row_count = total_monthyear
            progress.status = IngestorSessionStatus.SUCCESS
            progress.save()
            self.metadata['end_date'] = iter_monthyear

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
