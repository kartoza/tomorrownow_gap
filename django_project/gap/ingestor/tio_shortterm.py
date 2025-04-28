# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term ingestor.
"""

import json
import logging
import os
import traceback
import uuid
import zipfile
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da
import geohash
import duckdb
import time
from typing import List
from datetime import timedelta, date, datetime, time as time_s
import pytz
from concurrent.futures import ThreadPoolExecutor
from django.contrib.gis.geos import Point

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage, storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.contrib.gis.db.models.functions import Centroid
from django.utils import timezone

from core.utils.s3 import zip_folder_in_s3
from gap.ingestor.base import (
    BaseIngestor,
    BaseZarrIngestor,
    CoordMapping
)
from gap.ingestor.exceptions import (
    MissingCollectorSessionException, FileNotFoundException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    CastType, CollectorSession, DataSourceFile, DatasetStore, Grid,
    IngestorSession, Dataset, DatasetTimeStep
)
from gap.providers import TomorrowIODatasetReader
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset
from gap.utils.reader import DatasetReaderInput
from gap.utils.zarr import BaseZarrReader
from gap.utils.netcdf import find_start_latlng
from gap.utils.dask import execute_dask_compute
from gap.utils.geometry import ST_X, ST_Y


logger = logging.getLogger(__name__)


def path(filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}tio-short-term-collector/{filename}'


def trigger_task_after_ingestor_completed():
    """Trigger DCAS after T.io ingestor is completed."""
    from dcas.tasks import run_dcas
    run_dcas.delay()


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


class TioShortTermIngestor(BaseZarrIngestor):
    """Ingestor Tio Short Term data into Zarr."""

    DATE_VARIABLE = 'forecast_date'
    default_chunks = {
        'forecast_date': 10,
        'forecast_day_idx': 21,
        'lat': 150,
        'lon': 110
    }

    variables = [
        'total_rainfall',
        'total_evapotranspiration_flux',
        'max_temperature',
        'min_temperature',
        'precipitation_probability',
        'humidity_maximum',
        'humidity_minimum',
        'wind_speed_avg',
        'solar_radiation',
        'weather_code',
        'flood_index'
    ]

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize TioShortTermIngestor."""
        super().__init__(session, working_dir)

        self.metadata = {
            'chunks': [],
            'total_json_processed': 0
        }

        # min+max are the BBOX that GAP processes
        self.lat_metadata = {
            'min': -27,
            'max': 16,
            'inc': 0.03586314,
            'original_min': -4.65013565
        }
        self.lon_metadata = {
            'min': 21.8,
            'max': 52,
            'inc': 0.036353,
            'original_min': 33.91823667
        }
        self.reindex_tolerance = 0.001
        self.existing_dates = None

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )

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
            self.default_chunks['forecast_day_idx'],
            lat_len,
            lon_len
        )

    def get_chunks_for_forecast_date(self, is_single_date=True):
        """Get chunks for forecast date."""
        if not is_single_date:
            return (
                self.default_chunks['forecast_date'],
                self.default_chunks['forecast_day_idx'],
                self.default_chunks['lat'],
                self.default_chunks['lon']
            )
        return (
            1,
            self.default_chunks['forecast_day_idx'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

    def get_data_var_coordinates(self):
        """Get coordinates for data variables."""
        return ['forecast_date', 'forecast_day_idx', 'lat', 'lon']

    def get_coordinates(self, forecast_date: date, new_lat, new_lon):
        """Get coordinates for the dataset."""
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        forecast_day_indices = np.arange(-6, 15, 1)
        return {
            'forecast_date': ('forecast_date', forecast_date_array),
            'forecast_day_idx': (
                'forecast_day_idx', forecast_day_indices),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }

    def get_region_slices(
        self, forecast_date: date, nearest_lat_indices, nearest_lon_indices
    ):
        """Get region slices for update_by_region method."""
        # open existing zarr
        ds = self._open_zarr_dataset()

        # find index of forecast_date
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        new_forecast_date = forecast_date_array[0]
        forecast_date_idx = (
            np.where(ds['forecast_date'].values == new_forecast_date)[0][0]
        )

        ds.close()

        return {
            'forecast_date': slice(
                forecast_date_idx, forecast_date_idx + 1),
            'forecast_day_idx': slice(None),
            'lat': slice(
                nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
            'lon': slice(
                nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
        }

    def _append_new_forecast_date(
            self, forecast_date: date, is_new_dataset=False):
        """Append a new forecast date to the zarr structure.

        The dataset will be initialized with empty values.
        :param forecast_date: forecast date
        :type forecast_date: date
        """
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
            'forecast_date': {
                'chunks': self.default_chunks['forecast_date']
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
                zarr_url, mode='a', append_dim='forecast_date',
                consolidated=True,
                storage_options=self.s3_options,
                compute=False
            )
        execute_dask_compute(x)

        # close dataset and remove empty_data
        ds.close()
        del ds
        del empty_data

    def _update_by_region(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], new_data: dict):
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
                nearest_lon_arr
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
                nearest_lon_indices
            ),
            storage_options=self.s3_options,
            consolidated=True,
            compute=False
        )
        execute_dask_compute(x)

    def _run(self):
        """Process the tio shortterm data into Zarr."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)
        data_source = collector.dataset_files.first()
        if not data_source:
            raise FileNotFoundException()

        # find forecast date
        if 'forecast_date' not in data_source.metadata:
            raise AdditionalConfigNotFoundException('metadata.forecast_date')
        self.metadata['forecast_date'] = data_source.metadata['forecast_date']
        forecast_date = date.fromisoformat(
            data_source.metadata['forecast_date'])
        if not self._is_date_in_zarr(forecast_date):
            self._append_new_forecast_date(forecast_date, self.created)

        # get lat and lon array from grids
        lat_arr = set()
        lon_arr = set()
        grid_dict = {}

        # query grids
        grids = Grid.objects.annotate(
            centroid=Centroid('geometry')
        )
        for grid in grids:
            lat = round(grid.centroid.y, 8)
            lon = round(grid.centroid.x, 8)
            grid_hash = geohash.encode(lat, lon, precision=8)
            lat_arr.add(lat)
            lon_arr.add(lon)
            grid_dict[grid_hash] = grid.id
        lat_arr = sorted(lat_arr)
        lon_arr = sorted(lon_arr)

        # transform lat lon arrays
        lat_arr = self._transform_coordinates_array(lat_arr, 'lat')
        lon_arr = self._transform_coordinates_array(lon_arr, 'lon')

        lat_indices = [lat.nearest_idx for lat in lat_arr]
        lon_indices = [lon.nearest_idx for lon in lon_arr]
        assert self._is_sorted_and_incremented(lat_indices)
        assert self._is_sorted_and_incremented(lon_indices)

        # create slices for chunks
        lat_slices = self._find_chunk_slices(
            len(lat_arr), self.default_chunks['lat'])
        lon_slices = self._find_chunk_slices(
            len(lon_arr), self.default_chunks['lon'])

        # open zip file and process the data by chunks
        with default_storage.open(data_source.name) as _file:
            with zipfile.ZipFile(_file, 'r') as zip_file:
                for lat_slice in lat_slices:
                    for lon_slice in lon_slices:
                        lat_chunks = lat_arr[lat_slice]
                        lon_chunks = lon_arr[lon_slice]
                        warnings, count = self._process_tio_shortterm_data(
                            forecast_date, lat_chunks, lon_chunks,
                            grid_dict, zip_file
                        )
                        self.metadata['chunks'].append({
                            'lat_slice': str(lat_slice),
                            'lon_slice': str(lon_slice),
                            'warnings': warnings
                        })
                        self.metadata['total_json_processed'] += count

        # update end date of zarr datasource file
        self._update_zarr_source_file(forecast_date)

        # remove temporary source file
        remove_temp_file = self.get_config('remove_temp_file', True)
        if remove_temp_file:
            self._remove_temporary_source_file(data_source, data_source.name)

        # invalidate zarr cache
        self._invalidate_zarr_cache()

    def run(self):
        """Run TomorrowIO Ingestor."""
        # Run the ingestion
        is_success = False
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
            is_success = True
        except Exception as e:
            logger.error('Ingestor TomorrowIO failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            if is_success:
                trigger_task_after_ingestor_completed()

    def _process_tio_shortterm_data(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], grids: dict,
            zip_file: zipfile.ZipFile) -> dict:
        """Process Tio data and update into zarr.

        :param forecast_date: forecast date
        :type forecast_date: date
        :param lat_arr: list of latitude
        :type lat_arr: List[CoordMapping]
        :param lon_arr: list of longitude
        :type lon_arr: List[CoordMapping]
        :param grids: dictionary for geohash and grid id
        :type grids: dict
        :param zip_file: zip file from collector
        :type zip_file: zipfile.ZipFile
        :return: dictionary of warnings
        :rtype: dict
        """
        zip_file_list = zip_file.namelist()
        count = 0
        data_shape = (
            1,
            self.default_chunks['forecast_day_idx'],
            len(lat_arr),
            len(lon_arr)
        )
        warnings = {
            'missing_hash': 0,
            'missing_json': 0,
            'invalid_json': 0
        }

        # initialize empty new data for each variable
        new_data = {}
        for variable in self.variables:
            new_data[variable] = np.full(data_shape, np.nan, dtype='f8')

        for idx_lat, lat in enumerate(lat_arr):
            for idx_lon, lon in enumerate(lon_arr):
                # find grid id by geohash of lat and lon
                grid_hash = geohash.encode(lat.value, lon.value, precision=8)
                if grid_hash not in grids:
                    warnings['missing_hash'] += 1
                    continue

                # open the grid json file using grid id from grid_hash
                json_filename = f'grid-{grids[grid_hash]}.json'
                if json_filename not in zip_file_list:
                    warnings['missing_json'] += 1
                    continue

                with zip_file.open(json_filename) as _file:
                    data = json.loads(_file.read().decode('utf-8'))

                # there might be invalid json (e.g. API returns error)
                if 'data' not in data:
                    warnings['invalid_json'] += 1
                    continue

                # iterate for each item in data
                assert (
                    len(data['data']) ==
                    self.default_chunks['forecast_day_idx']
                )
                forecast_day_idx = 0
                for item in data['data']:
                    values = item['values']
                    for var in values:
                        if var not in new_data:
                            continue
                        # assign the variable value into new data
                        new_data[var][
                            0, forecast_day_idx, idx_lat, idx_lon] = (
                                values[var]
                        )
                    forecast_day_idx += 1
                count += 1

        # update new data to zarr using region
        self._update_by_region(forecast_date, lat_arr, lon_arr, new_data)
        del new_data

        return warnings, count


class TioShortTermDuckDBCollector(BaseIngestor):
    """Collector for Tio Short Term data."""

    TIME_STEP = DatasetTimeStep.DAILY

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioShortTermCollector."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()
        today = timezone.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Retrieve D-6 to D+14
        # Total days: 21
        self.start_dt = today - timedelta(days=6)
        self.end_dt = today + timedelta(days=15)
        self.forecast_date = today
        self.forecast_attrs = self.dataset.datasetattribute_set.filter(
            dataset__type__type=CastType.FORECAST
        ).order_by('attribute_id')
        self.num_threads = self.get_config('duckdb_num_threads', 4)
        self.attribute_names = [
            f.attribute.variable_name for f in self.forecast_attrs
        ]

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return tomorrowio_shortterm_forecast_dataset()

    def _init_dataset_files(self, chunks):
        data_source_ids = []
        if self.session.dataset_files.count() > 0:
            if self.session.dataset_files.count() != len(chunks):
                # need to ensure dataset files has the same length with chunks
                raise ValueError('Invalid size of existing dataset_files!')
            for ds_file in self.session.dataset_files.order_by('id').all():
                data_source_ids.append(ds_file.id)
        else:
            data_sources = []
            for chunk in chunks:
                data_source = DataSourceFile.objects.create(
                    name=f'{str(uuid.uuid4())}.duckdb',
                    dataset=self.dataset,
                    start_date_time=self.start_dt,
                    end_date_time=self.end_dt,
                    format=DatasetStore.DUCKDB,
                    created_on=timezone.now(),
                    metadata={
                        'forecast_date': (
                            self.forecast_date.date().isoformat()
                        ),
                        'total_grid': len(chunk),
                        'start_grid_id': chunk[0]['id'],
                        'end_grid_id': chunk[-1]['id'],
                    }
                )
                data_sources.append(data_source)
                data_source_ids.append(data_source.id)
            self.session.dataset_files.set(data_sources)

        return data_source_ids

    def _run(self):
        """Run TomorrowIO ingestor."""
        grids = Grid.objects.annotate(
                centroid=Centroid('geometry')
        ).annotate(
            lat=ST_Y('centroid'),
            lon=ST_X('centroid')
        ).values('id', 'lat', 'lon')
        logger.info(f'Total grids {grids.count()}')
        grids = list(grids)
        chunks = list(self._chunk_list(grids))

        # init data source file
        data_source_ids = self._init_dataset_files(chunks)

        # process chunks
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = list(
                executor.map(
                    lambda args: self._process_chunk(*args),
                    zip(chunks, data_source_ids)
                )
            )

        # post-process the results
        for idx, item in enumerate(results):
            # log results
            logger.info(json.dumps(item))

            # upload duckdb files to s3
            data_source = DataSourceFile.objects.get(
                id=data_source_ids[idx]
            )
            self._upload_duckdb_file(data_source)

    def _fetch_data(self, point: Point):
        """Fetch T.io data."""
        # Get the data
        location_input = DatasetReaderInput.from_point(point)
        reader = TomorrowIODatasetReader(
            self.dataset,
            self.forecast_attrs,
            location_input,
            datetime.combine(
                self.start_dt, time=time_s(0, 0, 0), tzinfo=pytz.utc
            ),
            datetime.combine(
                self.end_dt, time=time_s(0, 0, 0), tzinfo=pytz.utc
            )
        )
        for attempt in range(3):
            try:
                reader.read()
                if reader.is_success():
                    values = reader.get_data_values()
                    return values.to_json(), None
                else:
                    if attempt < 2:
                        logger.warning(
                            f"Attempt {attempt + 1} failed: "
                            f"{reader.error_status_codes}"
                        )
                        time.sleep(3 ** attempt)
            except Exception as e:
                if attempt < 2:  # Retry for the first two attempts
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. Retrying..."
                    )
                    time.sleep(3 ** attempt)  # Exponential backoff
                else:
                    logger.error("Max retries reached. Failing.")
                    raise

        return None, reader.error_status_codes

    def _chunk_list(self, data):
        """Split the list into smaller chunks."""
        chunk_size = max(1, len(data) // self.num_threads)
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def _get_connection(self, data_source: DataSourceFile):
        duckdb_filepath = os.path.join(
            self.working_dir, f'{data_source.name}'
        )
        conn = duckdb.connect(duckdb_filepath)
        return conn

    def _get_duckdb_filesize(self, data_source: DataSourceFile):
        duckdb_filepath = os.path.join(
            self.working_dir, f'{data_source.name}'
        )
        return os.stat(duckdb_filepath).st_size

    def _get_file_remote_url(self, filename):
        # use gap products dir prefix
        output_url = os.environ.get(
            'MINIO_GAP_AWS_DIR_PREFIX', '')
        if not output_url.endswith('/'):
            output_url += '/'
        output_url += f'tio_collector/{filename}'

        return output_url

    def _upload_duckdb_file(self, data_source: DataSourceFile):
        duckdb_filepath = os.path.join(
            self.working_dir, f'{data_source.name}'
        )
        s3_storage: S3Boto3Storage = storages["gap_products"]
        remote_url = self._get_file_remote_url(data_source.name)
        with open(duckdb_filepath, 'rb') as f:
            s3_storage.save(remote_url, f)

        # update metadata with its full remote_url
        data_source.metadata['remote_url'] = remote_url
        data_source.save()

    def _init_table(self, conn: duckdb.DuckDBPyConnection):
        attrib_cols = [f'{attr} DOUBLE' for attr in self.attribute_names]
        conn.execute(f"""
            CREATE SEQUENCE IF NOT EXISTS id_sequence;
            CREATE TABLE IF NOT EXISTS weather (
                id BIGINT PRIMARY KEY DEFAULT nextval('id_sequence'),
                grid_id BIGINT,
                lat DOUBLE,
                lon DOUBLE,
                date DATE,
                time TIME,
                {', '.join(attrib_cols)}
            )
        """)

    def _should_skip_date(self, date: date):
        """Skip insert to table for given date."""
        return False

    def _process_chunk(self, chunk, data_source_id):
        """Process chunk of grid."""
        # retrieve data source object
        data_source = DataSourceFile.objects.get(id=data_source_id)
        # create connection
        conn = self._get_connection(data_source)
        # init table
        self._init_table(conn)

        count_processed = 0
        count_error = 0
        status_codes_error = {}
        error_grids = []
        for grid in chunk:
            if self.is_cancelled():
                break
            grid_id = grid['id']

            # check if grid_id exists
            count = conn.execute(
                "SELECT COUNT(*) FROM weather WHERE grid_id=?", (grid_id,)
            ).fetchone()[0]
            if count > 0:
                continue

            # fetch data
            lat = grid['lat']
            lon = grid['lon']
            api_result, error_codes = self._fetch_data(Point(x=lon, y=lat))

            if error_codes:
                for status_code, count in error_codes.items():
                    if status_code in status_codes_error:
                        status_codes_error[status_code] += count
                    else:
                        status_codes_error[status_code] = count

            if api_result is None:
                logger.warning(f'failed to fetch data for grid {grid_id}')
                error_grids.append(grid_id)
                continue

            param_names = []
            param_placeholders = []
            for attr in self.attribute_names:
                param_names.append(attr)
                param_placeholders.append('?')

            data = api_result['data']
            batch_values = []
            for item in data:
                # insert into table
                values = item['values']
                dt = datetime.fromisoformat(item['datetime'])
                if self._should_skip_date(dt.date()):
                    continue

                param = [
                    grid_id, lat, lon, dt.date()
                ]

                # add time value
                if self.TIME_STEP == DatasetTimeStep.HOURLY:
                    param.append(dt.time())
                else:
                    param.append(
                        time_s(0, 0, 0, tzinfo=pytz.utc)
                    )

                for attr in self.attribute_names:
                    if attr in values:
                        param.append(values[attr])
                    else:
                        param.append(None)

                batch_values.append(param)

            # execute many
            conn.executemany(f"""
                INSERT INTO weather (grid_id, lat, lon, date, time,
                    {', '.join(param_names)}
                ) VALUES (?, ?, ?, ?, ?, {', '.join(param_placeholders)})
                """, batch_values
            )
            count_processed += 1

        conn.close()

        metadata_result = {
            'count_processed': count_processed,
            'count_error': count_error,
            'status_codes_error': status_codes_error,
            'error_grids': error_grids,
            'file_size': self._get_duckdb_filesize(data_source)
        }

        data_source.metadata.update(metadata_result)
        data_source.save()

        return metadata_result

    def run(self):
        """Run Tio Short Term Ingestor."""
        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            logger.error('Collector Tio Short Term failed!')
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass


class TioShortTermDuckDBIngestor(TioShortTermIngestor):
    """Collector for Tio Short Term data using DuckDB."""

    TIME_STEP = DatasetTimeStep.DAILY

    def _get_connection(self, collector: CollectorSession):
        """Download connection files and merge into 1 file."""
        duckdb_filepath = os.path.join(
            self.working_dir, f'{str(uuid.uuid4())}'
        )
        conn = duckdb.connect(duckdb_filepath)
        self._init_table(conn)

        column_names = ['grid_id', 'lat', 'lon', 'date', 'time']
        for variable in self.variables:
            column_names.append(variable)

        s3_storage: S3Boto3Storage = storages["gap_products"]
        for dataset_file in collector.dataset_files.all():
            remote_path = dataset_file.metadata['remote_url']
            output_path = os.path.join(
                self.working_dir, dataset_file.name
            )
            # Download the file
            with (
                s3_storage.open(remote_path, "rb") as remote_file,
                open(output_path, "wb") as local_file
            ):
                local_file.write(remote_file.read())

            # Attach the external DuckDB file
            conn.execute(
                f"ATTACH '{output_path}' AS ext_db_{dataset_file.id}"
            )

            # Insert data from the external DuckDB table
            conn.execute(
                f"""
                INSERT INTO weather ({', '.join(column_names)})
                SELECT {', '.join(column_names)}
                FROM ext_db_{dataset_file.id}.weather
                """
            )

        return conn

    def _init_table(self, conn: duckdb.DuckDBPyConnection):
        attrib_cols = [f'{attr} DOUBLE' for attr in self.variables]
        conn.execute(
            f"""
            CREATE SEQUENCE IF NOT EXISTS id_sequence;
            CREATE TABLE IF NOT EXISTS weather (
                id BIGINT PRIMARY KEY DEFAULT nextval('id_sequence'),
                grid_id BIGINT,
                lat DOUBLE,
                lon DOUBLE,
                date DATE,
                time TIME,
                {', '.join(attrib_cols)}
            )
            """
        )

    def _fetch_data(self, conn, grid_ids):
        df = conn.sql(
            f"""
            SELECT * FROM weather
            WHERE grid_id IN {grid_ids}
            ORDER BY grid_id asc, date, time asc
            """
        ).to_df()
        if df.shape[0] > 0:
            return df
        return None

    def _process_tio_shortterm_data_from_conn(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], grids: dict,
            conn: duckdb.DuckDBPyConnection) -> dict:
        """Process Tio data and update into zarr.

        :param forecast_date: forecast date
        :type forecast_date: date
        :param lat_arr: list of latitude
        :type lat_arr: List[CoordMapping]
        :param lon_arr: list of longitude
        :type lon_arr: List[CoordMapping]
        :param grids: dictionary for geohash and grid id
        :type grids: dict
        :return: dictionary of warnings
        :rtype: dict
        """
        count = 0
        data_shape = self.get_empty_shape(
            len(lat_arr), len(lon_arr)
        )
        warnings = {
            'missing_hash': 0,
            'missing_json': 0,
            'invalid_json': 0
        }

        # initialize empty new data for each variable
        new_data = {}
        for variable in self.variables:
            new_data[variable] = np.full(data_shape, np.nan, dtype='f8')

        grid_ids = {}
        for idx_lat, lat in enumerate(lat_arr):
            for idx_lon, lon in enumerate(lon_arr):
                # find grid id by geohash of lat and lon
                grid_hash = geohash.encode(lat.value, lon.value, precision=8)
                if grid_hash not in grids:
                    warnings['missing_hash'] += 1
                    continue

                grid_ids[f'{idx_lat}_{idx_lon}'] = grids[grid_hash]

        # load weather by grid_ids
        weather_df = self._fetch_data(conn, list(grid_ids.values()))

        if weather_df is None:
            return warnings, count

        for idx_lat, lat in enumerate(lat_arr):
            for idx_lon, lon in enumerate(lon_arr):
                grid_key = f'{idx_lat}_{idx_lon}'
                if grid_key not in grid_ids:
                    # skip because missing_hash
                    continue

                grid_id = grid_ids[grid_key]
                df = (
                    weather_df[weather_df["grid_id"] == grid_id].sort_values(
                        by="date"
                    )
                )
                # open the grid json file using grid id from grid_hash
                if df.shape[0] == 0:
                    warnings['missing_json'] += 1
                    continue

                # validate total days
                row_count = df.shape[0]
                if self.TIME_STEP == DatasetTimeStep.HOURLY:
                    row_count = len(df['date'].unique())
                assert (
                    row_count ==
                    self.default_chunks['forecast_day_idx']
                )

                # iterate for each item in data
                forecast_day_idx = 0
                current_dt = None
                for index, item in df.iterrows():
                    if current_dt is None:
                        # initialize first date
                        current_dt = item['date']

                    time_idx = None
                    if (
                        self.TIME_STEP == DatasetTimeStep.HOURLY and
                        item['time'] is not None
                    ):
                        # Convert to index 0–23
                        time_idx = item['time'].hour

                    for var in self.variables:
                        if var not in df.columns:
                            continue
                        # assign the variable value into new data
                        if self.TIME_STEP == DatasetTimeStep.HOURLY:
                            if time_idx is not None:
                                new_data[var][
                                    0, forecast_day_idx, time_idx,
                                    idx_lat, idx_lon
                                ] = item[var]
                        else:
                            new_data[var][
                                0, forecast_day_idx, idx_lat, idx_lon] = (
                                    item[var]
                            )
                    if current_dt != item['date']:
                        forecast_day_idx += 1
                        current_dt = item['date']
                count += 1

        # update new data to zarr using region
        self._update_by_region(forecast_date, lat_arr, lon_arr, new_data)
        del new_data

        return warnings, count

    def _run(self):
        """Process the tio shortterm data into Zarr."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)
        data_source = collector.dataset_files.first()
        if not data_source:
            raise FileNotFoundException()

        # find forecast date
        if 'forecast_date' not in data_source.metadata:
            raise AdditionalConfigNotFoundException('metadata.forecast_date')
        self.metadata['forecast_date'] = data_source.metadata['forecast_date']
        forecast_date = date.fromisoformat(
            data_source.metadata['forecast_date'])
        if not self._is_date_in_zarr(forecast_date):
            self._append_new_forecast_date(forecast_date, self.created)

        # get connection
        conn = self._get_connection(collector)

        # get lat and lon array from grids
        lat_arr = set()
        lon_arr = set()
        grid_dict = {}

        # query grids
        grids = Grid.objects.annotate(
            centroid=Centroid('geometry')
        )
        for grid in grids:
            lat = round(grid.centroid.y, 8)
            lon = round(grid.centroid.x, 8)
            grid_hash = geohash.encode(lat, lon, precision=8)
            lat_arr.add(lat)
            lon_arr.add(lon)
            grid_dict[grid_hash] = grid.id
        lat_arr = sorted(lat_arr)
        lon_arr = sorted(lon_arr)

        # transform lat lon arrays
        lat_arr = self._transform_coordinates_array(lat_arr, 'lat')
        lon_arr = self._transform_coordinates_array(lon_arr, 'lon')

        lat_indices = [lat.nearest_idx for lat in lat_arr]
        lon_indices = [lon.nearest_idx for lon in lon_arr]
        assert self._is_sorted_and_incremented(lat_indices)
        assert self._is_sorted_and_incremented(lon_indices)

        # create slices for chunks
        lat_slices = self._find_chunk_slices(
            len(lat_arr), self.default_chunks['lat'])
        lon_slices = self._find_chunk_slices(
            len(lon_arr), self.default_chunks['lon'])

        # process the data by chunks
        for lat_slice in lat_slices:
            for lon_slice in lon_slices:
                lat_chunks = lat_arr[lat_slice]
                lon_chunks = lon_arr[lon_slice]
                warnings, count = self._process_tio_shortterm_data_from_conn(
                    forecast_date, lat_chunks, lon_chunks,
                    grid_dict, conn
                )
                self.metadata['chunks'].append({
                    'lat_slice': str(lat_slice),
                    'lon_slice': str(lon_slice),
                    'warnings': warnings
                })
                self.metadata['total_json_processed'] += count

        # update end date of zarr datasource file
        self._update_zarr_source_file(forecast_date)

        # remove temporary source file
        remove_temp_file = self.get_config('remove_temp_file', True)
        if remove_temp_file:
            self._remove_source_files(collector)

        # invalidate zarr cache
        self._invalidate_zarr_cache()

    def _remove_source_files(self, collector: CollectorSession):
        s3_storage: S3Boto3Storage = storages["gap_products"]
        for dataset_file in collector.dataset_files.all():
            remote_path = dataset_file.metadata['remote_url']
            s3_storage.delete(remote_path)
            dataset_file.delete()
