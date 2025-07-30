# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Generator using TAMSAT data
"""

import logging
import os
import duckdb
import pandas as pd
import shutil
from django.utils import timezone
from django.conf import settings

from core.models.object_storage_manager import ObjectStorageManager
from gap.models import (
    Preferences,
    FarmGroup
)
from core.utils.url_file_checker import file_exists_at_url
from core.utils.s3 import s3_file_exists
from spw.models import SPWExecutionLog, SPWMethod
from spw.tamsat.planting_date_api import (
    routine_operations_v2,
    get_pfc_filename,
    WRSI_FILENAME
)

logger = logging.getLogger(__name__)
DEFAULT_MAX_CHUNK_SIZE = 5000


class TamsatSPWGenerator:
    """SPW Generator for TAMSAT data."""

    SPW_TMP_TABLE_NAME = 'spw_tamsat_tmp'
    SPW_TABLE_NAME = 'spw_tamsat'

    def __init__(self, date=None):
        """Initialize the TamsatSPWGenerator."""
        self.date = date or timezone.now()
        self.working_dir = os.path.join(
            '/tmp',
            f'spw_tamsat_{self.date.isoformat()}'
        )
        os.makedirs(self.working_dir, exist_ok=True)
        self.preferences = Preferences.load()
        self.spw_logs = {}

    def _init_config(self):
        """Initialize configuration for the SPW generator."""
        self.config = self.preferences.crop_plan_config.get(
            'tamsat_spw_config', {}
        )
        group_filter_names = self.config.get(
            'farm_groups', []
        )
        self.farm_groups = FarmGroup.objects.all()
        if group_filter_names:
            logger.info(
                f"Filtering SPW farm groups by names: {group_filter_names}"
            )
            self.farm_groups = self.farm_groups.filter(
                name__in=group_filter_names
            )

        if not self.farm_groups.exists():
            raise ValueError(
                "No farm groups found for the specified configuration."
            )

        # Init dictionary for farm group and its request object
        self.spw_logs = {}
        for farm_group in self.farm_groups:
            self.spw_logs[farm_group.id] = {
                'farm_group': farm_group,
                'log': SPWExecutionLog.init_record(
                    farm_group,
                    self.date,
                    SPWMethod.TAMSAT
                )
            }

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
        self.tamsat_url = self.preferences.tamsat_url
        if not self.tamsat_url:
            raise ValueError('TAMSAT URL not found in preferences.')

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
        self.chunk_size = self.config.get(
            'chunk_size',
            DEFAULT_MAX_CHUNK_SIZE
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

    def _get_connection(self):
        config = self._get_duckdb_config(self.s3)
        dudckdb_path = os.path.join(
            self.working_dir, 'tamsat_spw.duckdb'
        )
        conn = duckdb.connect(dudckdb_path, config=config)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.install_extension("postgres")
        conn.load_extension("postgres")

        # Create a temporary table for SPW data
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.SPW_TMP_TABLE_NAME} (
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
                sm_user_decision DOUBLE
            )
        """)

        return conn

    def _get_current_month_parquet_path(self):
        """Get the path for the current month's geoparquet file."""
        return (
            f"s3://{self.s3['S3_BUCKET_NAME']}/"
            f"{self.s3['S3_DIR_PREFIX']}/{self.geoparquet_path}/"
            f"year={self.date.year}/month={self.date.month}.parquet"
        )

    def _check_parquet_exists(self):
        """Check if the geoparquet file exists."""
        s3_client = ObjectStorageManager.get_s3_client(self.s3)
        path = (
            f"{self.s3['S3_DIR_PREFIX']}/{self.geoparquet_path}/"
            f"year={self.date.year}/month={self.date.month}.parquet"
        )

        return s3_file_exists(s3_client, self.s3['S3_BUCKET_NAME'], path)

    def _pull_existing_monthly_data(self, conn):
        """Pull existing monthly data from the geoparquet file."""
        parquet_path = self._get_current_month_parquet_path()
        conn.execute(
            f"""
            CREATE TABLE {self.SPW_TABLE_NAME} AS
            SELECT * FROM read_parquet(
                '{parquet_path}'
            )
            """
        )

    def _insert_farm_group_data(self, conn, df):
        """Insert df to the SPW temporary table."""
        if df.empty:
            logger.warning("No data to insert into SPW temporary table.")
            return

        if 'geometry' not in df.columns:
            df['geometry'] = None

        # reorder columns to match the SPW temporary table
        df = df[[
            'date', 'farm_id', 'farm_unique_id', 'country',
            'farm_group_id', 'farm_group', 'grid_id', 'grid_unique_id',
            'geometry', 'latitude', 'longitude',
            'sm_25', 'sm_50', 'sm_70', 'spw_20',
            'spw_40', 'spw_60', 'pfc_user_probability',
            'wrsi_user_probability', 'pfc_user_decision',
            'wrsi_user_decision', 'sm_user_decision'
        ]]

        conn.execute(
            f"""
            INSERT INTO {self.SPW_TMP_TABLE_NAME}
            FROM df
            """
        )

    def _prepare_geometry(self, conn):
        """Prepare geometry from latitude and longitude columns."""
        conn.execute(f"""
            UPDATE {self.SPW_TMP_TABLE_NAME}
            SET geometry = ST_SetSRID(
                ST_MakePoint(longitude, latitude), 4326
            )
            WHERE geometry IS NULL;
        """)

    def _move_to_spw_table(self, conn):
        """Move data from temporary table to SPW table."""
        conn.execute(
            f"""
                INSERT INTO {self.SPW_TABLE_NAME}
                SELECT * FROM {self.SPW_TMP_TABLE_NAME}
            """
        )

    def _store_as_geoparquet(self, conn):
        """Store the SPW data as a geoparquet file."""
        # Get boundaries from SPW table
        bbox = conn.execute(
            f"""
            SELECT ST_Extent(geometry) FROM {self.SPW_TABLE_NAME}
            """
        ).fetchone()[0]

        # Order by date and farm geometry
        sql = (
            f"""
            CREATE TABLE spw_ordered AS
            SELECT *
            FROM {self.SPW_TABLE_NAME}
            ORDER BY date,
            ST_Hilbert(
                geometry,
                ST_Extent(ST_MakeEnvelope(
                {bbox[0]}, {bbox[1]},
                {bbox[2]}, {bbox[3]}
                ))
            );
            """
        )
        conn.execute(sql)

        # Export to GeoParquet
        parquet_path = self._get_current_month_parquet_path()
        sql = (
            f"""
            COPY (
                SELECT * FROM spw_ordered
            ) TO '{parquet_path}'
            (FORMAT 'parquet', COMPRESSION 'zstd');
            """
        )
        conn.execute(sql)

    def _get_slices(self, total_count, max_chunk_size=DEFAULT_MAX_CHUNK_SIZE):
        """Yield slices of farm groups."""
        for start in range(0, total_count, max_chunk_size):
            end = min(start + max_chunk_size, total_count)
            yield slice(start, end)

    def _execute_spw_model(
        self, farm_group: FarmGroup, farms_slice, chunk_idx
    ):
        """Execute Tamsat SPW model for a farm group."""
        # Create df with columns:
        # date, farm_id, farm_unique_id, country, farm_group_id,
        # farm_group, grid_id, grid_unique_id, Latitude, Longitude,
        df = pd.DataFrame({
            'date': self.date,
            'farm_id': None,
            'farm_unique_id': None,
            'country': None,
            'farm_group_id': farm_group.id,
            'farm_group': farm_group.name,
            'grid_id': None,
            'grid_unique_id': None,
            'latitude': None,
            'longitude': None,
        })

        # Add farm data to df
        for farm in farms_slice:
            df = df.append(
                {
                    'farm_id': farm.id,
                    'farm_unique_id': farm.unique_id,
                    'country': farm.grid.country.name if farm.grid else None,
                    'grid_id': farm.grid.id if farm.grid else None,
                    'grid_unique_id': (
                        farm.grid.unique_id if farm.grid else None
                    ),
                    'latitude': farm.geometry.y,
                    'longitude': farm.geometry.x,
                },
                ignore_index=True
            )

        if df.empty:
            logger.warning(
                f"No farms found for farm group: {farm_group.name} "
                f"with chunk index: {chunk_idx}"
            )
            return

        # export to csv file
        csv_file_path = os.path.join(
            self.working_dir,
            f'spw_tamsat_{farm_group.id}_{chunk_idx}.csv'
        )

        pfc_thresh = self.config.get(
            'pfc_thresh', 70
        )
        pfc_prob_thresh = self.config.get(
            'pfc_prob_thresh', 0.8
        )
        wrsi_thresh_factor = self.config.get(
            'wrsi_thresh_factor', 0.75
        )
        wrsi_prob_thresh = self.config.get(
            'wrsi_prob_thresh', 0.5
        )

        logger.info(
            f'Processing farm group: {farm_group.name} '
            f'with chunk index: {chunk_idx} - '
            f'with parameters: pfc_thresh={pfc_thresh}, '
            f'pfc_prob_thresh={pfc_prob_thresh}, '
            f'wrsi_thresh_factor={wrsi_thresh_factor}, '
            f'wrsi_prob_thresh={wrsi_prob_thresh}'
        )

        all_df, basic_df = routine_operations_v2(
            self.date.year, self.date.month, self.date.day, csv_file_path,
            self.tamsat_url, self.working_dir,
            farm_group.name.replace(' ', ''),
            pfc_thresh=pfc_thresh,
            pfc_prob_thresh=pfc_prob_thresh,
            wrsi_thresh_factor=wrsi_thresh_factor,
            wrsi_prob_thresh=wrsi_prob_thresh,
            csv_output=False
        )

        return all_df

    def _process_farm_group(self, conn, farm_group: FarmGroup):
        """Process a single farm group."""
        logger.info(f"Processing farm group: {farm_group.name}")
        farms = (
            farm_group.farms.select_related('grid', 'grid__country')
            .all().order_by('id')
        )
        total_count = farms.count()

        if total_count == 0:
            logger.warning(
                f"No farms found for farm group: {farm_group.name}"
            )
            return

        # Remove existing data for the farm group in current date
        conn.execute(
            f"""
            DELETE FROM {self.SPW_TMP_TABLE_NAME}
            WHERE farm_group_id = {farm_group.id}
            AND date = '{self.date.date()}';
            """
        )

        # Process farms in chunks
        all_slices = self._get_slices(total_count, self.chunk_size)
        chunk_idx = 0
        for slice_ in all_slices:
            chunk_idx += 1
            logger.info(
                f"Processing chunk {chunk_idx}/{len(all_slices)} "
                f"for farm group: {farm_group.name}"
            )
            farms_slice = farms[slice_]
            df = self._execute_spw_model(
                farm_group, farms_slice, chunk_idx
            )
            if df.empty:
                continue

            self._insert_farm_group_data(conn, df)

    def _cleanup(self):
        """Cleanup temporary files and tables."""
        # Cleanup working directory
        if os.path.exists(self.working_dir):
            shutil.rmtree(self.working_dir)

    def _update_spw_error_logs(self, error_message, farm_group_id=None):
        """Update SPW error logs."""
        if farm_group_id:
            spw_log = self.spw_logs.get(farm_group_id)
            if spw_log:
                spw_log['log'].stop_with_error(error_message)
                logger.error(
                    "SPW generation failed for farm group: "
                    f"{spw_log['farm_group'].name} "
                    f"with error: {error_message}"
                )
        else:
            logger.error(
                f"Error occurred while processing SPW for date: {self.date}."
            )
            # update all logs
            for spw_log in self.spw_logs.values():
                spw_log['log'].stop_with_error(error_message)

    def _check_spw_resources_exists(self):
        """Check if the SPW resources exist."""
        pfc_file_exists = file_exists_at_url(
            self.tamsat_url + get_pfc_filename(self.date) + '.gz'
        )
        if not pfc_file_exists:
            self._update_spw_error_logs(
                f"TAMSAT PFC file does not exist for date: {self.date}"
            )
            raise FileNotFoundError(
                f"TAMSAT PFC file does not exist for date: {self.date}"
            )

        wrsi_file_exists = file_exists_at_url(
            self.tamsat_url + WRSI_FILENAME
        )
        if not wrsi_file_exists:
            self._update_spw_error_logs(
                f"TAMSAT WRSI file does not exist for date: {self.date}"
            )
            raise FileNotFoundError(
                f"TAMSAT WRSI file does not exist for date: {self.date}"
            )

    def _run(self):
        """Run the SPW generator with Tamsat Alert."""
        # Initialize configuration
        self._init_config()

        # Check if SPW resources exist
        self._check_spw_resources_exists()

        # Get connection to DuckDB
        conn = self._get_connection()

        # Check if geoparquet file exists
        if self._check_parquet_exists():
            logger.info(
                "Geoparquet file already exists for the current month. "
                "Pulling existing data."
            )
            self._pull_existing_monthly_data(conn)

        # Process each farm group
        for farm_group in self.farm_groups:
            self._process_farm_group(conn, farm_group)

        # Prepare geometry
        self._prepare_geometry(conn)

        # Move data to SPW table
        self._move_to_spw_table(conn)

        # Store as geoparquet
        self._store_as_geoparquet(conn)

        logger.info("SPW generation completed successfully.")
        conn.close()
        self._cleanup()

    def run(self):
        """Run the SPW generator."""
        try:
            self._run()
        except Exception as e:
            logger.error(
                f"Error running Tamsat SPW generator: {e}",
                exc_info=True
            )
            raise e
