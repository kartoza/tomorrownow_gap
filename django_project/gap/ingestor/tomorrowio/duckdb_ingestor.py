# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term JSON Ingestor.
"""

import logging
import os
import uuid
import numpy as np
import geohash
import duckdb
import time
import pandas as pd
from typing import List
from datetime import date

from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.contrib.gis.db.models.functions import Centroid

from gap.ingestor.base import (
    CoordMapping
)
from gap.ingestor.exceptions import (
    MissingCollectorSessionException, FileNotFoundException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    CollectorSession, Grid,
    DatasetTimeStep, Preferences,
    IngestorSessionStatus,
    Dataset, DatasetStore
)
from gap.ingestor.tomorrowio.json_ingestor import TioShortTermIngestor


logger = logging.getLogger(__name__)


class TioShortTermDuckDBIngestor(TioShortTermIngestor):
    """Collector for Tio Short Term data using DuckDB."""

    TIME_STEP = DatasetTimeStep.DAILY

    def _get_connection(self, collector: CollectorSession):
        """Download connection files and merge into 1 file."""
        duckdb_filepath = os.path.join(
            self.working_dir, f'{str(uuid.uuid4())}'
        )
        config = {
            'threads': self.get_config(
                'duckdb_config_threads',
                Preferences.load().duckdb_threads_num
            ),
            'memory_limit': self.get_config(
                'duckdb_config_memory_limit',
                '256MB'
            )
        }
        conn = duckdb.connect(duckdb_filepath, config=config)
        wal_autocheckpoint = self.get_config(
            'duckdb_wal_autocheckpoint',
            '64MB'
        )
        conn.execute(f"PRAGMA wal_autocheckpoint='{wal_autocheckpoint}'")
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
        lat_chunk_size = self.get_config(
            'lat_chunk_size',
            self.default_chunks['lat']
        )
        lon_chunk_size = self.get_config(
            'lon_chunk_size',
            self.default_chunks['lon']
        )
        lat_slices = self._find_chunk_slices(
            len(lat_arr), lat_chunk_size
        )
        lon_slices = self._find_chunk_slices(
            len(lon_arr), lon_chunk_size
        )

        total_progress = (
            len(lat_slices) * len(lon_slices)
        )
        progress = self._add_progress(
            f'Processing {forecast_date.isoformat()} - '
            f'{total_progress} chunks',
        )
        start_time = time.time()
        total_processed = 0

        # process the data by chunks
        for lat_slice in lat_slices:
            for lon_slice in lon_slices:
                chunk_progress = self._add_progress(
                    f'Chunk {total_processed + 1}/'
                    f'{total_progress}'
                )
                chunk_start_time = time.time()
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

                total_processed += 1
                chunk_progress.notes = (
                    f"Execution time: {time.time() - chunk_start_time}"
                )
                chunk_progress.status = IngestorSessionStatus.SUCCESS
                chunk_progress.save()

        # close connection
        conn.close()

        # update end date of zarr datasource file
        self._update_zarr_source_file(forecast_date)

        # remove temporary source file
        remove_temp_file = self.get_config('remove_temp_file', True)
        if remove_temp_file:
            self._remove_source_files(collector)

        # invalidate zarr cache
        self._invalidate_zarr_cache()

        # update progress
        total_time = time.time() - start_time
        progress.notes = f"Execution time: {total_time}"
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

    def _remove_source_files(self, collector: CollectorSession):
        s3_storage: S3Boto3Storage = storages["gap_products"]
        for dataset_file in collector.dataset_files.all():
            remote_path = dataset_file.metadata['remote_url']
            s3_storage.delete(remote_path)
            dataset_file.delete()


class TioHourlyShortTermIngestor(TioShortTermDuckDBIngestor):
    """Ingestor for Tio Hourly Short-Term forecast data."""

    TIME_STEP = DatasetTimeStep.HOURLY
    TRIGGER_DCAS = False
    default_chunks = {
        'forecast_date': 10,
        'forecast_day_idx': 4,
        'time': 24,
        'lat': 20,
        'lon': 20
    }
    variables = [
        'total_rainfall',
        'total_evapotranspiration_flux',
        'temperature',
        'precipitation_probability',
        'humidity',
        'wind_speed',
        'solar_radiation',
        'weather_code',
        'flood_index',
        'wind_direction'
    ]

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioHourlyShortTermIngestor."""
        super().__init__(session, working_dir)

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
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
            self.default_chunks['time'],
            lat_len,
            lon_len
        )

    def get_chunks_for_forecast_date(self, is_single_date=True):
        """Get chunks for forecast date."""
        if not is_single_date:
            return (
                self.default_chunks['forecast_date'],
                self.default_chunks['forecast_day_idx'],
                self.default_chunks['time'],
                self.default_chunks['lat'],
                self.default_chunks['lon']
            )
        return (
            1,
            self.default_chunks['forecast_day_idx'],
            self.default_chunks['time'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

    def get_data_var_coordinates(self):
        """Get coordinates for data variables."""
        return ['forecast_date', 'forecast_day_idx', 'time', 'lat', 'lon']

    def get_coordinates(self, forecast_date: date, new_lat, new_lon):
        """Get coordinates for the dataset."""
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        # generate 4 days of forecast
        forecast_day_indices = np.arange(1, 5, 1)
        times = np.array([np.timedelta64(h, 'h') for h in range(24)])
        return {
            'forecast_date': ('forecast_date', forecast_date_array),
            'forecast_day_idx': (
                'forecast_day_idx', forecast_day_indices),
            'time': ('time', times),
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
            'time': slice(None),
            'lat': slice(
                nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
            'lon': slice(
                nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
        }
