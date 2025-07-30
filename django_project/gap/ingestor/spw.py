# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm ingestor.
"""

import time
from django.contrib.gis.db.models import Union
import duckdb
from django.db import connection
from django.conf import settings

from core.models.object_storage_manager import ObjectStorageManager
from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    NoDataException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    IngestorSession,
    Preferences,
    FarmSuitablePlantingWindowSignal,
    FarmGroup
)


class SPWIngestor(BaseIngestor):
    """Ingestor for SPW monthly data to GeoParquet files."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize SPW Ingestor."""
        super().__init__(session, working_dir)
        self.use_ssl = self.get_config('use_ssl', None)

    def _init_config(self):
        self.month = self.get_config('month')
        self.year = self.get_config('year')
        if not self.month or not self.year:
            raise AdditionalConfigNotFoundException(
                'Month and year'
            )
        self.preferences = Preferences.load()
        self.geoparquet_path = self.preferences.crop_plan_config.get(
            'geoparquet_path', None
        )
        if not self.geoparquet_path:
            raise AdditionalConfigNotFoundException(
                'Geoparquet path (crop_plan_config)'
            )
        self.geoparquet_connection_name = (
            self.preferences.crop_plan_config.get(
                'geoparquet_connection_name', 'default'
            )
        )
        self.s3 = ObjectStorageManager.get_s3_env_vars(
            self.geoparquet_connection_name
        )
        self.duck_db_num_threads = self.get_config(
            'duck_db_num_threads',
            1
        )
        self.duckdb_memory_limit = self.get_config(
            'duckdb_memory_limit',
            '1GB'
        )

    def _get_duckdb_config(self, s3):
        endpoint = s3['S3_ENDPOINT_URL']
        # Remove protocol from endpoint
        endpoint = endpoint.replace('http://', '')
        endpoint = endpoint.replace('https://', '')
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]

        use_ssl = not settings.DEBUG
        if self.use_ssl is not None:
            use_ssl = self.use_ssl

        config = {
            's3_access_key_id': s3['S3_ACCESS_KEY_ID'],
            's3_secret_access_key': s3['S3_SECRET_ACCESS_KEY'],
            's3_region': 'us-east-1',
            's3_url_style': 'path',
            's3_endpoint': endpoint,
            's3_use_ssl': use_ssl,
            'memory_limit': self.duckdb_memory_limit,
        }
        if self.duck_db_num_threads:
            config['threads'] = self.duck_db_num_threads

        return config

    def _get_connection(self, s3):
        config = self._get_duckdb_config(s3)
        conn = duckdb.connect(config=config)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.install_extension("postgres")
        conn.load_extension("postgres")
        return conn

    def _get_farms_boundaries(self):
        combined_bbox = FarmSuitablePlantingWindowSignal.objects.filter(
            generated_date__year=self.year,
            generated_date__month=self.month
        ).aggregate(
            combined_geometry=Union('farm__geometry')
        )
        return combined_bbox['combined_geometry'].extent

    def _get_parquet_path(self):
        return (
            f"s3://{self.s3['S3_BUCKET_NAME']}/"
            f"{self.s3['S3_DIR_PREFIX']}/{self.geoparquet_path}/"
            f"year={self.year}/month={self.month}.parquet"
        )

    def _execute_sql(self, conn, sql, description):
        progress = self._add_progress(description)
        start_time = time.time()
        conn.execute(sql)
        elapsed_time = time.time() - start_time
        progress.notes = (
            f"SQL executed in {elapsed_time:.2f} seconds."
        )
        progress.save()

    def _insert_by_farm_group(self, conn, farm_group: FarmGroup):
        # get total farms in the farm group
        sql = (
            f"""
            SELECT COUNT(*)
            FROM pg_conn.gap_farmsuitableplantingwindowsignal spw
            WHERE EXTRACT(YEAR FROM spw.generated_date) = {self.year}
            AND EXTRACT(MONTH FROM spw.generated_date) = {self.month}
            AND spw.farm_id IN (
                SELECT farm_id FROM pg_conn.gap_farmgroup_farms
                WHERE farmgroup_id = {farm_group.id}
            )
            """
        )
        total_records = conn.sql(sql).fetchone()[0]

        # Fetch  data from pg_conn
        sql = (
            f"""
            INSERT INTO spw_signal
            SELECT *
            FROM postgres_query(
                'pg_conn',
            'SELECT
                spw.generated_date as date,
                f.id AS farm_id,
                f.unique_id AS farm_unique_id,
                c.name AS country,
                ''{farm_group.name}'' AS farm_group,
                {farm_group.id} AS farm_group_id,
                g.id AS grid_id,
                g.unique_id AS grid_unique_id,
                ST_AsEWKT(f.geometry) AS geometry,
                spw.signal,
                spw.last_2_days,
                spw.last_4_days,
                spw.today_tomorrow,
                spw.too_wet_indicator
            FROM public.gap_farmsuitableplantingwindowsignal spw
            JOIN public.gap_farm f ON spw.farm_id = f.id
            LEFT JOIN public.gap_grid g ON f.grid_id = g.id
            LEFT JOIN public.gap_country c ON g.country_id = c.id
            WHERE EXTRACT(YEAR FROM spw.generated_date) = {self.year}
            AND EXTRACT(MONTH FROM spw.generated_date) = {self.month}
            AND f.id IN (
                SELECT farm_id FROM public.gap_farmgroup_farms
                WHERE farmgroup_id = {farm_group.id}
            )
            ORDER BY spw.generated_date'
            )
            """
        )
        self._execute_sql(
            conn, sql,
            f'Inserting data for farm group {farm_group.name} '
            f'({total_records} records)'
        )

    def _run(self):
        self._init_config()

        total_count = FarmSuitablePlantingWindowSignal.objects.filter(
            generated_date__year=self.year,
            generated_date__month=self.month
        ).count()
        if total_count == 0:
            raise NoDataException(f'{self.month}-{self.year}', 'SPW')

        # Get farms boundaries
        farms_boundaries = self._get_farms_boundaries()

        # init duckdb connection
        conn = self._get_connection(self.s3)
        pg_conn_str = (
            "host={HOST} port={PORT} user={USER} "
            "password={PASSWORD} dbname={NAME}".format(
                **connection.settings_dict
            )
        )
        conn.execute(f"""
            ATTACH '{pg_conn_str}' AS pg_conn
            (TYPE postgres, READ_ONLY, SCHEMA 'public');
        """)

        create_table_query = (
            """
            CREATE TABLE IF NOT EXISTS spw_signal (
                date DATE,
                farm_id BIGINT,
                farm_unique_id VARCHAR,
                country VARCHAR,
                farm_group VARCHAR,
                farm_group_id BIGINT,
                grid_id BIGINT,
                grid_unique_id VARCHAR,
                geometry GEOMETRY,
                signal VARCHAR,
                last_2_days DOUBLE,
                last_4_days DOUBLE,
                today_tomorrow DOUBLE,
                too_wet_indicator VARCHAR
            )
            """
        )
        self._execute_sql(
            conn, create_table_query,
            'Creating spw_signal table if not exists'
        )

        # Fetch data for each farm group
        farm_groups = FarmGroup.objects.all()
        for farm_group in farm_groups:
            self._insert_by_farm_group(conn, farm_group)

        # Order by date and farm geometry
        sql = (
            f"""
            CREATE TABLE spw_signal_ordered AS
            SELECT *
            FROM spw_signal
            ORDER BY date,
            ST_Hilbert(
                geometry,
                ST_Extent(ST_MakeEnvelope(
                {farms_boundaries[0]}, {farms_boundaries[1]},
                {farms_boundaries[2]}, {farms_boundaries[3]}
                ))
            );
            """
        )
        self._execute_sql(
            conn, sql,
            'Ordering spw_signal by date and farm geometry'
        )

        # Export to GeoParquet
        sql = (
            f"""
            COPY (
                SELECT * FROM spw_signal_ordered
            ) TO '{self._get_parquet_path()}'
            (FORMAT 'parquet', COMPRESSION 'zstd');
            """
        )
        self._execute_sql(
            conn, sql,
            'Exporting spw_signal to GeoParquet'
        )

        # Clean up
        conn.execute("DETACH pg_conn;")
        conn.close()

    def run(self):
        """Run the ingestor."""
        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            raise Exception(e)
