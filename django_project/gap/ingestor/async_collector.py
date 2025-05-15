# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Async Collector to pull data from API.
"""

import logging
import os
import uuid
import pandas as pd
import duckdb
import random
import asyncio
import aiohttp
import redis.asyncio as aioredis
from aiolimiter import AsyncLimiter
from datetime import datetime, time as time_s
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.contrib.gis.db.models.functions import Centroid
from django.utils import timezone

from gap.ingestor.base import (
    BaseIngestor
)
from gap.models import (
    CastType, CollectorSession, DataSourceFile, DatasetStore, Grid,
    Dataset, DatasetTimeStep, Preferences
)
from gap.utils.geometry import ST_X, ST_Y
from gap.utils.api import mask_api_key_from_error


logger = logging.getLogger(__name__)


class AsyncCollector(BaseIngestor):
    """Base Collector for pulling data from API."""

    CANCEL_KEY_TEMPLATE = '{}_cancel_{}'
    INGESTOR_NAME = None
    TIME_STEP = DatasetTimeStep.DAILY
    # Batch Size to insert data into DuckDB
    DEFAULT_BATCH_SIZE = 500
    # Maximum number of retries for API requests
    DEFAULT_MAX_RETRIES = 3
    # Default rate limit per second
    DEFAULT_RATE_LIMIT_PER_SECOND = 70
    # Default maximum concurrent requests
    DEFAULT_MAX_CONCURRENT_REQUESTS = 30
    # Default base URL
    DEFAULT_BASE_URL = None
    # DEFAULT MAX SIZE OF QUEUE
    DEFAULT_MAX_QUEUE_SIZE = 1000
    # DIRECTORY FOR REMOTE URL
    DEFAULT_REMOTE_URL_DIR = 'duckdb_collector'

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize AsyncCollector."""
        # use separate working dir that persist in tmp
        working_dir = f'/tmp/{self.INGESTOR_NAME}'
        if not os.path.exists(working_dir):
            os.makedirs(working_dir, exist_ok=True)
        super().__init__(session, working_dir)

        # Dataset configuration
        self.dataset = self._init_dataset()
        self.total_grid = 0
        self.dataset_attrs = self.dataset.datasetattribute_set.select_related(
            'attribute'
        ).filter(
            dataset__type__type=CastType.FORECAST
        ).order_by('attribute_id')
        self.attribute_names = [
            f.attribute.variable_name for f in self.dataset_attrs
        ]
        self.attribute_requests = [
            f.source for f in self.dataset_attrs
        ]

        # Configurations
        self.redis = aioredis.from_url(
            f'redis://default:{os.environ.get("REDIS_PASSWORD", "")}'
            f'@{os.environ.get("REDIS_HOST", "")}'
        )
        self.queue = asyncio.Queue(
            maxsize=self.get_config(
                'max_queue_size',
                self.DEFAULT_MAX_QUEUE_SIZE
            )
        )
        self.batch_size = self.get_config(
            'batch_size', self.DEFAULT_BATCH_SIZE
        )
        self.cancel_key = self.CANCEL_KEY_TEMPLATE.format(
            self.INGESTOR_NAME,
            self.session.id
        )
        self.max_retries = self.get_config(
            'max_retries', self.DEFAULT_MAX_RETRIES
        )
        self.in_flight_semaphore = asyncio.Semaphore(
            self.get_config(
                'max_concurrent_requests',
                self.DEFAULT_MAX_CONCURRENT_REQUESTS
            )
        )
        self.rate_limit_per_second = self.get_config(
            'rate_limit_per_second',
            self.DEFAULT_RATE_LIMIT_PER_SECOND
        )
        self.rate_limiter = AsyncLimiter(
            self.rate_limit_per_second,
            time_period=1
        )

        # API Configuration
        self.base_url = self.get_config(
            'base_url', self.DEFAULT_BASE_URL
        )
        today = timezone.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        self.start_dt: datetime = None
        self.end_dt: datetime = None
        self.forecast_date: datetime = None
        self._init_dates(today)

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        raise NotImplementedError(
            "This method should be implemented in the subclass."
        )

    def _init_dates(self, today: datetime):
        """Initialize start and end dates."""
        raise NotImplementedError(
            "This method should be implemented in the subclass."
        )

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
        output_url += f'{self.DEFAULT_REMOTE_URL_DIR}/{filename}'

        return output_url

    def _upload_duckdb_file(self, data_source: DataSourceFile):
        duckdb_filepath = os.path.join(
            self.working_dir, f'{data_source.name}'
        )
        s3_storage: S3Boto3Storage = storages["gap_products"]
        remote_url = self._get_file_remote_url(data_source.name)
        # check if file exists
        if s3_storage.exists(remote_url):
            # delete existing file
            s3_storage.delete(remote_url)

        with open(duckdb_filepath, 'rb') as f:
            s3_storage.save(remote_url, f)

        # update metadata with its full remote_url
        data_source.metadata['remote_url'] = remote_url
        data_source.metadata['file_size'] = (
            self._get_duckdb_filesize(data_source)
        )
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

    def check_upstream(self, data_source: DataSourceFile):
        """Check if upstream data is available."""
        if 'remote_url' not in data_source.metadata:
            return
        remote_path = data_source.metadata['remote_url']
        s3_storage: S3Boto3Storage = storages["gap_products"]
        if not s3_storage.exists(remote_path):
            return
        output_path = os.path.join(
            self.working_dir, f'{data_source.name}'
        )
        # Download the file
        with (
            s3_storage.open(remote_path, "rb") as remote_file,
            open(output_path, "wb") as local_file
        ):
            local_file.write(remote_file.read())
        logger.info(
            f"Resuming {remote_path}..."
        )

    def _get_connection(self, data_source: DataSourceFile):
        duckdb_filepath = os.path.join(
            self.working_dir, f'{data_source.name}'
        )
        # if not exist, then check upstream
        if not os.path.exists(duckdb_filepath):
            self.check_upstream(data_source)
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

    def _get_existing_grid_ids(self, con):
        return con.sql(
            "SELECT DISTINCT grid_id FROM weather"
        ).to_df()

    def _init_dataset_files(self):
        data_sources = []
        if self.session.dataset_files.count() > 0:
            # use existing dataset files
            data_sources = self.session.dataset_files.order_by('id').all()
        else:
            data_sources.append(
                DataSourceFile.objects.create(
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
                        'total_grid': self.total_grid
                    }
                )
            )
            self.session.dataset_files.set(data_sources)

        return data_sources

    def get_payload_for_grid(
        self, grid, start_date: datetime, end_date: datetime
    ):
        """Get payload for Tomorrow.io API request."""
        raise NotImplementedError(
            "This method should be implemented in the subclass."
        )

    def get_api_url(self):
        """Get API URL to fetch data."""
        raise NotImplementedError(
            "This method should be implemented in the subclass."
        )

    def get_dataframe_from_batch(self, batch):
        """Get dataframe from batch of data."""
        raise NotImplementedError(
            "This method should be implemented in the subclass."
        )

    async def reset_cancellation_flag(self):
        """Reset the cancellation flag in Redis."""
        try:
            await self.redis.set(self.cancel_key, 0, ex=60 * 60 * 24)
        except Exception as e:
            logger.error(f"Error resetting cancellation flag: {e}")

    async def set_cancellation_flag(self):
        """Set the cancellation flag in Redis."""
        try:
            await self.redis.set(self.cancel_key, 1, ex=60 * 60 * 24)
        except Exception as e:
            logger.error(f"Error set cancellation flag: {e}")

    async def check_cancellation_flag(self):
        """Check the cancellation flag in Redis."""
        flag = await self.redis.get(self.cancel_key)
        return flag == b"1"

    async def fetch_producer(self, grid, payload):
        """Fetch data from external API."""
        headers = {
            'Accept-Encoding': 'gzip',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        url = self.get_api_url()
        grid_id = grid['id']
        lat = grid['lat']
        lon = grid['lon']
        last_ex = None
        async with self.in_flight_semaphore:
            attempt = 0
            while attempt < self.max_retries:
                if await self.check_cancellation_flag():
                    logger.info("[Producer] Cancelled from Redis...")
                    return

                try:
                    async with self.rate_limiter:
                        async with aiohttp.ClientSession() as session:
                            async with (
                                session.post(
                                    url, json=payload, headers=headers
                                )
                            ) as response:
                                response.raise_for_status()
                                data = await response.json()
                                await self.queue.put({
                                    "grid_id": grid_id,
                                    "data": data,
                                    "lat": lat,
                                    "lon": lon
                                })
                                return

                except Exception as e:
                    logger.error(
                        f"[Producer] Error fetching data for {grid_id}: "
                        f"{mask_api_key_from_error(str(e))}"
                    )
                    attempt += 1
                    wait_time = 2 ** attempt + random.uniform(0, 1)
                    await asyncio.sleep(wait_time)
                    last_ex = e

        logger.error(
            f"[Producer] Failed after {self.max_retries} attempts "
            f"for grid_id {grid_id}: {last_ex}"
        )

    def _filter_date_df(self, df: pd.DataFrame):
        # Filter date less than start date
        df = df[df['date'] >= self.start_dt.date()]
        # Filter date greater than end date
        df = df[df['date'] < self.end_dt.date()]
        return df

    def _insert_batch(self, con, batch):
        try:
            df = self.get_dataframe_from_batch(batch)
            # Split datetime into date and time columns
            df['date'] = pd.to_datetime(df['datetime']).dt.date
            if self.TIME_STEP == DatasetTimeStep.HOURLY:
                df['time'] = pd.to_datetime(df['datetime']).dt.time
            else:
                df['time'] = time_s(0, 0, 0)
            # Drop the original datetime column
            df.drop(columns=['datetime'], inplace=True)
            # filter date
            df = self._filter_date_df(df)
            # add missing columns
            rename_dict = {}
            for f in self.dataset_attrs:
                attr = f.source
                if attr not in df.columns:
                    df[attr] = None
                rename_dict[attr] = f.attribute.variable_name
            df.rename(columns=rename_dict, inplace=True)

            con.execute(f"""
                INSERT INTO weather ({','.join(df.columns)})
                SELECT * FROM df
            """)
        except Exception as e:
            logger.error(
                f"[Consumer] Error during batch insert: {e}",
                exc_info=True
            )
            raise e

    async def insert_batch_async(self, con, batch):
        """Insert batch into DuckDB."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._insert_batch, con, batch)

    async def consumer(self, con):
        """Consumer to process data from the queue."""
        batch = []
        while True:
            if await self.check_cancellation_flag():
                logger.info(
                    "[Consumer] Cancelled from Redis, flushing and exiting."
                )
                break

            try:
                data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                batch.append(data)
                self.queue.task_done()

                if len(batch) >= self.batch_size:
                    await self.insert_batch_async(con, batch)
                    batch = []
            except asyncio.TimeoutError:
                pass

            # if self.queue.qsize() % 100 == 0 and self.queue.qsize() > 0:
            #     logger.info(f"[Consumer] Queue size: {self.queue.qsize()}")

        # Flush remaining
        if batch:
            await self.insert_batch_async(con, batch)

        logger.info("[Consumer] Exited gracefully.")

    async def _run_async(self, grids, con):
        """Run the async tasks."""
        # reset cancellation flag
        await self.reset_cancellation_flag()
        consumer_task = asyncio.create_task(self.consumer(con))
        producer_tasks = []
        for grid in grids:
            payload = self.get_payload_for_grid(
                grid, self.start_dt, self.end_dt
            )
            producer_task = asyncio.create_task(
                self.fetch_producer(grid, payload)
            )
            producer_tasks.append(producer_task)

        await asyncio.gather(*producer_tasks, return_exceptions=True)

        # Allow consumer to finish remaining
        await self.queue.join()

        # Cancel the consumer task
        await asyncio.sleep(10)
        await self.set_cancellation_flag()

        try:
            await consumer_task
        except asyncio.CancelledError:
            logger.info("[Runner] Consumer task cancelled cleanly.")

        # close redis connection
        await self.redis.aclose()

    def _run(self):
        """Run Async Collector."""
        # init data source file
        self._init_dataset_files()

        con = self._get_connection(self.session.dataset_files.first())
        self._init_table(con)

        # check existing grid ids
        existing_grid_ids = self._get_existing_grid_ids(con)

        _grids = Grid.objects.annotate(
                centroid=Centroid('geometry')
        ).annotate(
            lat=ST_Y('centroid'),
            lon=ST_X('centroid')
        ).values('id', 'lat', 'lon')
        self.total_grid = _grids.count()
        grids = []
        for grid in _grids:
            grid_id = grid['id']
            # check if grid_id exists
            exists = existing_grid_ids[
                existing_grid_ids['grid_id'] == grid_id
            ].shape[0]
            if exists > 0:
                continue
            grids.append(grid)
        logger.info(f"Total grids: {len(grids)}/{self.total_grid}")

        asyncio.run(self._run_async(grids, con))

        # Close the connection
        con.close()
        # upload duckdb file
        self._upload_duckdb_file(self.session.dataset_files.first())

    def run(self):
        """Run the Async Collector."""
        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            logger.error(
                f'Collector {self.INGESTOR_NAME} failed!',
                exc_info=True
            )
            raise Exception(e)
