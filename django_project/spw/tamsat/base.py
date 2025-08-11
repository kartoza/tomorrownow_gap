# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Generator using TAMSAT data
"""

from django.conf import settings

from core.models.object_storage_manager import ObjectStorageManager
from gap.models import (
    Preferences
)
from core.utils.s3 import s3_file_exists


class TamsatSPWBase:
    """Base class for TAMSAT SPW operations."""

    SPW_TABLE_NAME = 'spw_tamsat'
    CREATE_TABLE_QUERY = (
        """
            CREATE TABLE IF NOT EXISTS {} (
                date DATE,
                farm_id BIGINT,
                farm_unique_id VARCHAR,
                country VARCHAR,
                farm_group_id BIGINT,
                farm_group VARCHAR,
                grid_id BIGINT,
                grid_unique_id VARCHAR,
                geometry GEOMETRY,
                latitude DOUBLE,
                longitude DOUBLE,
                sm_25 DOUBLE,
                sm_50 DOUBLE,
                sm_70 DOUBLE,
                spw_20 DOUBLE,
                spw_40 DOUBLE,
                spw_60 DOUBLE,
                pfc_user_probability DOUBLE,
                wrsi_user_probability DOUBLE,
                pfc_user_decision DOUBLE,
                wrsi_user_decision DOUBLE,
                sm_user_decision DOUBLE,
                pfc_thresh DOUBLE,
                pfc_prob_thresh DOUBLE,
                wrsi_thresh_factor DOUBLE,
                wrsi_prob_thresh DOUBLE
            )
        """
    )

    def __init__(self, verbose=False):
        """Initialize the TamsatSPWBase."""
        self.preferences = Preferences.load()
        self.config = self.preferences.crop_plan_config.get(
            'tamsat_spw_config', {}
        )
        self.verbose = verbose

    def _init_config(self):
        """Initialize configuration for the SPW generator."""
        self.geoparquet_path = self.config.get(
            'geoparquet_path', None
        )
        if not self.geoparquet_path:
            raise ValueError(
                'Tamsat geoparquet path not found in preferences.'
            )

        self.geoparquet_connection_name = (
            self.config.get(
                'geoparquet_connection_name', 'default'
            )
        )

        self.s3 = ObjectStorageManager.get_s3_env_vars(
            self.geoparquet_connection_name
        )
        self.duck_db_num_threads = self.config.get(
            'duck_db_num_threads',
            1
        )
        self.duckdb_memory_limit = self.config.get(
            'duckdb_memory_limit',
            '1GB'
        )

    def _get_duckdb_config(self):
        endpoint = self.s3['S3_ENDPOINT_URL']
        # Remove protocol from endpoint
        endpoint = endpoint.replace('http://', '')
        endpoint = endpoint.replace('https://', '')
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]

        use_ssl = not settings.DEBUG

        config = {
            's3_access_key_id': self.s3['S3_ACCESS_KEY_ID'],
            's3_secret_access_key': self.s3['S3_SECRET_ACCESS_KEY'],
            's3_region': 'us-east-1',
            's3_url_style': 'path',
            's3_endpoint': endpoint,
            's3_use_ssl': use_ssl,
            'memory_limit': self.duckdb_memory_limit,
        }
        if self.duck_db_num_threads:
            config['threads'] = self.duck_db_num_threads

        return config

    def _get_current_month_parquet_path(self, date):
        """Get the path for the current month's geoparquet file."""
        return (
            f"s3://{self.s3['S3_BUCKET_NAME']}/"
            f"{self.s3['S3_DIR_PREFIX']}/{self.geoparquet_path}/"
            f"year={date.year}/month={date.month}.parquet"
        )

    def _check_parquet_exists(self, date):
        """Check if the geoparquet file exists."""
        s3_client = ObjectStorageManager.get_s3_client(self.s3)
        path = (
            f"{self.s3['S3_DIR_PREFIX']}/{self.geoparquet_path}/"
            f"year={date.year}/month={date.month}.parquet"
        )

        return s3_file_exists(s3_client, self.s3['S3_BUCKET_NAME'], path)

    def _pull_existing_monthly_data(self, conn, date):
        """Pull existing monthly data from the geoparquet file."""
        parquet_path = self._get_current_month_parquet_path(date)
        conn.execute(
            f"DELETE FROM {self.SPW_TABLE_NAME};"
        )
        conn.execute(
            f"""
            INSERT INTO {self.SPW_TABLE_NAME}
            SELECT * EXCLUDE(year) FROM read_parquet(
                '{parquet_path}'
            )
            """
        )

    def count_rows_in_spw_table(self, conn, table_name):
        """Count the number of rows in the SPW table."""
        if not self.verbose:
            return 0

        count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        print(f"There are {count} rows in {table_name}")
        return count
