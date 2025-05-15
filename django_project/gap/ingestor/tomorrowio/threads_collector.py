# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term ingestor using threads.
"""

import json
import logging
import os
import traceback
import uuid
import duckdb
import time
from datetime import timedelta, date, datetime, time as time_s
import pytz
from concurrent.futures import ThreadPoolExecutor
from django.contrib.gis.geos import Point

from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.contrib.gis.db.models.functions import Centroid
from django.utils import timezone

from gap.ingestor.base import (
    BaseIngestor
)
from gap.models import (
    CastType, CollectorSession, DataSourceFile, DatasetStore, Grid,
    Dataset, DatasetTimeStep, Preferences,
    Provider, DatasetType
)
from gap.providers import TomorrowIODatasetReader
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset
from gap.utils.reader import DatasetReaderInput
from gap.utils.geometry import ST_X, ST_Y


logger = logging.getLogger(__name__)


class TioShortTermDuckDBCollector(BaseIngestor):
    """Collector for Tio Short Term data."""

    TIME_STEP = DatasetTimeStep.DAILY
    DEFAULT_GRID_BATCH_SIZE = 500

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioShortTermCollector."""
        # use separate working dir that persist in tmp
        working_dir = '/tmp/tio_collector'
        if not os.path.exists(working_dir):
            os.makedirs(working_dir, exist_ok=True)
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
        self.grid_batch_size = self.get_config(
            'grid_batch_size', self.DEFAULT_GRID_BATCH_SIZE
        )

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
                    values = reader.get_raw_results()
                    return values, None
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
        wal_autocheckpoint = self.get_config(
            'duckdb_wal_autocheckpoint',
            '64MB'
        )
        conn = duckdb.connect(
            duckdb_filepath,
            config=config
        )
        conn.execute(f"PRAGMA wal_autocheckpoint='{wal_autocheckpoint}'")
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
        # remove local file
        try:
            if os.path.exists(duckdb_filepath):
                os.remove(duckdb_filepath)
        except OSError as e:
            logger.error(
                f"Error deleting file {duckdb_filepath}: {e}",
                exc_info=True
            )

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

        # fetch existing grid_id
        grid_id_df = conn.sql(
            "SELECT DISTINCT grid_id FROM weather"
        ).to_df()

        conn.execute("BEGIN TRANSACTION")
        count_processed = 0
        count_error = 0
        status_codes_error = {}
        error_grids = []
        for grid in chunk:
            if self.is_cancelled():
                break
            grid_id = grid['id']

            # check if grid_id exists
            exists = grid_id_df[
                grid_id_df['grid_id'] == grid_id
            ].shape[0]
            if exists > 0:
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

            batch_values = []
            for item in api_result:
                # insert into table
                values = item.values
                dt = item.datetime
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
                    param.append(values.get(attr, None))
                batch_values.append(param)

            # execute many
            conn.executemany(f"""
                INSERT INTO weather (grid_id, lat, lon, date, time,
                    {', '.join(param_names)}
                ) VALUES (?, ?, ?, ?, ?, {', '.join(param_placeholders)})
                """, batch_values
            )
            count_processed += 1
            if count_processed % self.grid_batch_size == 0:
                conn.execute("COMMIT")
                conn.execute("BEGIN TRANSACTION")
                logger.info(f'Processed {count_processed} grids')

        conn.execute("COMMIT")
        conn.close()
        logger.info(f'Processed {count_processed} grids')

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


class TioShortTermHourlyDuckDBCollector(TioShortTermDuckDBCollector):
    """Collector for Tio Hourly Short-Term forecast data."""

    TIME_STEP = DatasetTimeStep.HOURLY

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioHourlyShortTermCollector."""
        super().__init__(session, working_dir)

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        provider = Provider.objects.get(name='Tomorrow.io')
        dt_shorttermforecast = DatasetType.objects.get(
            variable_name='cbam_shortterm_hourly_forecast',
            type=CastType.FORECAST
        )
        return Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
            provider=provider,
            type=dt_shorttermforecast,
            store_type=DatasetStore.EXT_API,
            time_step=DatasetTimeStep.HOURLY,
            is_internal_use=True
        )

    def _should_skip_date(self, date: date):
        """Skip insert to table for given date."""
        return date == self.end_dt.date()
