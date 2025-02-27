# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Tem ingestor.
"""

import json
import logging
import os
import traceback
import uuid
import zipfile
import time
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da
import geohash
import duckdb
from typing import List
from datetime import timedelta, date, datetime, time as time_s
import pytz
from concurrent.futures import ThreadPoolExecutor
from django.contrib.gis.geos import Point

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.db.models import FloatField
from django.contrib.gis.db.models.functions import Centroid, GeoFunc
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
    IngestorSession, Dataset, IngestorSessionStatus
)
from gap.providers import TomorrowIODatasetReader
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset
from gap.utils.reader import DatasetReaderInput
from gap.utils.zarr import BaseZarrReader
from gap.utils.netcdf import find_start_latlng
from gap.utils.dask import execute_dask_compute


logger = logging.getLogger(__name__)


class ST_X(GeoFunc):
    """Custom GeoFunc to extract lon."""

    output_field = FloatField()
    function = 'ST_X'


class ST_Y(GeoFunc):
    """Custom GeoFunc to extract lat."""

    output_field = FloatField()
    function = 'ST_Y'


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
            logger.error('Ingestor Tio Short Term failed!', e)
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass


class TioShortTermIngestor(BaseZarrIngestor):
    """Ingestor Tio Short Term data into Zarr."""

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
        'solar_radiation'
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

    def _is_date_in_zarr(self, date: date) -> bool:
        """Check whether a date has been added to zarr file.

        :param date: date to check
        :type date: date
        :return: True if date exists in zarr file.
        :rtype: bool
        """
        if self.created:
            return False
        if self.existing_dates is None:
            ds = self._open_zarr_dataset(self.variables)
            self.existing_dates = ds.forecast_date.values
            ds.close()
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in self.existing_dates

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
        empty_shape = (
            1,
            self.default_chunks['forecast_day_idx'],
            len(new_lat),
            len(new_lon)
        )
        chunks = (
            1,
            self.default_chunks['forecast_day_idx'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

        # Create the Dataset
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        forecast_day_indices = np.arange(-6, 15, 1)
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
                ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
                empty_data
            )
            encoding[var] = {
                'chunks': (
                    self.default_chunks['forecast_date'],
                    self.default_chunks['forecast_day_idx'],
                    self.default_chunks['lat'],
                    self.default_chunks['lon']
                )
            }
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'forecast_date': ('forecast_date', forecast_date_array),
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
        # open existing zarr
        ds = self._open_zarr_dataset()

        # find index of forecast_date
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        new_forecast_date = forecast_date_array[0]
        forecast_date_idx = (
            np.where(ds['forecast_date'].values == new_forecast_date)[0][0]
        )

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
                ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
                new_data[var]
            ) for var in new_data
        }
        new_ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'forecast_date': [new_forecast_date],
                'forecast_day_idx': ds['forecast_day_idx'],
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
                'forecast_date': slice(
                    forecast_date_idx, forecast_date_idx + 1),
                'forecast_day_idx': slice(None),
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


class TioHistoricalBackfillCollector(BaseIngestor):
    """Collector for Tio Short Term data.
    
    Note: this class should be run manually, e.g. from command.
    """

    def __init__(self, session: CollectorSession, start_date: date, end_date: date, working_dir: str = '/tmp', num_threads = 4):
        """Initialize TioHistoricalBackfillCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(
            provider__name='Tomorrow.io',
            name='Tomorrow.io Historical Reanalysis',
            type__type=CastType.HISTORICAL,
            store_type=DatasetStore.EXT_API
        )
        self.start_date = start_date
        self.end_date = end_date
        self.attributes = self.dataset.datasetattribute_set.all()
        self.error_status_codes = {}
        self.num_threads = num_threads
        if session.dataset_files.count() > 0:
            self.dataset_file = session.dataset_files.first()
        else:
            self.dataset_file = DataSourceFile.objects.create(
                dataset=self.dataset,
                start_date_time=datetime.combine(
                    start_date, time=time_s(0, 0, 0), tzinfo=pytz.utc
                ),
                end_date_time=datetime.combine(
                    end_date, time=time_s(0, 0, 0), tzinfo=pytz.utc
                ),
                format=DatasetStore.ZIP_FILE,
                name=f'{uuid.uuid4()}.duckdb',
                created_on=timezone.now()
            )
            self.session.dataset_files.set([self.dataset_file])
        self.duck_db_num_threads = 2
        self.conn = None

    def _get_time_interval(self):
        rps_per_thread = 100 / self.num_threads
        return 1 / rps_per_thread

    def _is_cancelled(self):
        file_path = os.path.join(
            '/tmp', 'tio_backfill', 'stop'
        )
        return os.path.exists(file_path)

    def _get_connection(self):
        temp_filepath = os.path.join(
            '/tmp', 'tio_backfill'
        )
        os.makedirs(temp_filepath, exist_ok=True)
        duckdb_filepath = os.path.join(
            temp_filepath,
            self.dataset_file.name
        )

        conn = duckdb.connect(duckdb_filepath)
        return conn

    def _init_table(self, conn):
        local_con = conn.cursor()
        local_con.execute("""
            CREATE SEQUENCE IF NOT EXISTS id_sequence;
            CREATE TABLE IF NOT EXISTS weather (
                id BIGINT PRIMARY KEY DEFAULT nextval('id_sequence'),
                grid_id BIGINT,
                lat DOUBLE,
                lon DOUBLE,
                date DATE,
                total_rainfall DOUBLE,
                total_evapotranspiration_flux DOUBLE,
                max_temperature DOUBLE,
                min_temperature DOUBLE,
                precipitation_probability DOUBLE,
                humidity_maximum DOUBLE,
                humidity_minimum DOUBLE,
                wind_speed_avg DOUBLE,
                solar_radiation DOUBLE,
            )
        """)

    def _fetch_data(self, lat, lon):
        """Fetch T.io data."""
        # Get the data
        location_input = DatasetReaderInput.from_point(
            Point(x=lon, y=lat)
        )
        reader = TomorrowIODatasetReader(
            self.dataset,
            self.attributes,
            location_input,
            datetime.combine(
                self.start_date, time=time_s(0, 0, 0), tzinfo=pytz.utc
            ),
            datetime.combine(
                self.end_date, time=time_s(0, 0, 0), tzinfo=pytz.utc
            )
        )
        reader.read()
        if reader.is_success():
            values = reader.get_data_values()
            return values.to_json(), None

        return None, reader.error_status_codes

    def _chunk_list(self, data):
        """Split the list into smaller chunks."""
        chunk_size = max(1, len(data) // self.num_threads)  # Ensure at least one element per thread
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def _process_chunk(self, chunk):
        """Function to process each chunk."""
        local_con = self.conn.cursor()
        total_grid = len(chunk)
        count_processed = 0
        count_error = 0
        status_codes_error = {}
        error_grids = []
        for grid in chunk:
            if self._is_cancelled():
                break
            grid_id = grid['id']
            count = local_con.execute(
                "SELECT COUNT(*) FROM weather WHERE grid_id=?", (grid_id,)
            ).fetchone()[0]
            if count > 0:
                # print(f'skip grid {grid_id}')
                continue
            # time.sleep(self._get_time_interval())
            lat = grid['lat']
            lon = grid['lon']
            api_result, error_codes = self._fetch_data(lat, lon)

            if error_codes:
                for status_code, count in error_codes.items():
                    if status_code in status_codes_error:
                        status_codes_error[status_code] += count
                    else:
                        status_codes_error[status_code] = count

            if api_result is None:
                print(f'failed to fetch data for grid {grid_id}')
                error_grids.append(grid_id)
                continue

            data = api_result['data']
            for item in data:
                # insert into table
                values = item['values']
                local_con.execute("""
                    INSERT INTO weather (grid_id, lat, lon, date,
                        total_rainfall, total_evapotranspiration_flux,
                        max_temperature, min_temperature,
                        humidity_maximum, humidity_minimum,
                        wind_speed_avg, solar_radiation, precipitation_probability
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    grid_id, lat, lon, datetime.fromisoformat(item['datetime']).date(),
                    values['total_rainfall'], values['total_evapotranspiration_flux'],
                    values['max_temperature'], values['min_temperature'],
                    values['humidity_maximum'], values['humidity_minimum'],
                    values['wind_speed_avg'], values['solar_radiation'],
                    values['precipitation_probability']
                ))
            count_processed += 1
        return {
            'total_grid': total_grid,
            'count_processed': count_processed,
            'count_error': count_error,
            'status_codes_error': status_codes_error,
            'error_grids': error_grids
        }

    def _run(self):
        """Run TomorrowIO ingestor."""
        self.conn = self._get_connection()
        self._init_table(self.conn)

        grids = Grid.objects.annotate(
            centroid=Centroid('geometry')
        ).annotate(
            lat=ST_Y('centroid'),
            lon=ST_X('centroid')
        ).values('id', 'lat', 'lon')
        print(f'Total grids {grids.count()}')
        grids = list(grids)
        chunks = list(self._chunk_list(grids))
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = list(executor.map(self._process_chunk, chunks))

        for item in results:
            print(item)

        self.conn.close()


class TioHistoricalBackfillIngestor(TioShortTermIngestor):

    def _get_connection(self, data_source: DataSourceFile):
        temp_filepath = os.path.join(
            '/tmp', 'tio_backfill'
        )
        os.makedirs(temp_filepath, exist_ok=True)
        duckdb_filepath = os.path.join(
            temp_filepath,
            data_source.name
        )

        conn = duckdb.connect(duckdb_filepath, config={
            'threads': 2
        })
        return conn

    def _get_all_dates(self, conn):
        dates = conn.sql(
            """
            select distinct date from weather order by date
            """
        ).fetchall()
        return [d[0] for d in dates]

    def _find_data_by_grid(self, conn, grid_id, date: date):
        df = conn.sql(
            f"SELECT * FROM weather WHERE grid_id={grid_id} AND date='{date.isoformat()}'"
        ).to_df()
        if df.shape[0] > 0:
            result = {}
            for attrib in self.variables:
                result[attrib] = df[attrib][0]
            return result
        return None

    def _find_data_by_date(self, conn, date: date):
        df = conn.sql(
            f"SELECT * FROM weather WHERE date='{date.isoformat()}'"
        ).to_df()
        if df.shape[0] > 0:
            grid_data = {}
            for row in df.itertuples(index=False):
                result = {}
                for attrib in self.variables:
                    result[attrib] = getattr(row, attrib, None)
                grid_id = getattr(row, 'grid_id')
                grid_data[grid_id] = result
            print(f'total grid {len(grid_data.keys())}')
            return grid_data
        return None

    def _run(self):
        """Process the tio shortterm data into Zarr."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)
        data_source = collector.dataset_files.first()
        if not data_source:
            raise FileNotFoundException()

        conn = self._get_connection(data_source)

        dates = self._get_all_dates(conn)
        # add new date after last one
        last_date = dates[-1] + timedelta(days=1)
        print(last_date)
        dates.append(last_date)
        for forecast_date in dates:
            if not self._is_date_in_zarr(forecast_date):
                print(f'Appending date {forecast_date}')
                self._append_new_forecast_date(forecast_date, self.created)
                if self.created:
                    self.created = False

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

        # open duckdb connection and process the data by chunks
        for forecast_date in dates:
            if forecast_date == last_date:
                continue
            print(f'Processing date {forecast_date}')
            grid_data = self._find_data_by_date(conn, forecast_date)
            for lat_slice in lat_slices:
                for lon_slice in lon_slices:
                    lat_chunks = lat_arr[lat_slice]
                    lon_chunks = lon_arr[lon_slice]
                    warnings, count = self._process_tio_shortterm_data_from_conn(
                        forecast_date, lat_chunks, lon_chunks,
                        grid_dict, grid_data
                    )
                    self.metadata['chunks'].append({
                        'lat_slice': str(lat_slice),
                        'lon_slice': str(lon_slice),
                        'warnings': warnings
                    })
                    self.metadata['total_json_processed'] += count

            # update end date of zarr datasource file
            self._update_zarr_source_file(forecast_date)

    def _process_tio_shortterm_data_from_conn(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], grids: dict,
            grid_data) -> dict:
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

                grid_id = grids[grid_hash]
                
                # data = self._find_data_by_grid(conn, grid_id, forecast_date)
                data = grid_data.get(grid_id, None)

                # there might be invalid json (e.g. API returns error)
                if data is None:
                    warnings['invalid_json'] += 1
                    continue

                # use forecast_day_idx = -1 which is at index 5
                forecast_day_idx = 5
                for var in data.keys():
                    if var not in new_data:
                        continue
                    # assign the variable value into new data
                    new_data[var][
                        0, forecast_day_idx, idx_lat, idx_lon] = (
                            data[var]
                    )
                    
                count += 1

        # update new data to zarr using region
        # update to the next date
        self._update_by_region(
            forecast_date + timedelta(days=1), lat_arr, lon_arr, new_data
        )
        del new_data

        return warnings, count

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
                self.session.status = IngestorSessionStatus.SUCCESS
            else:
                self.session.status = IngestorSessionStatus.FAILED
            self.session.save()
