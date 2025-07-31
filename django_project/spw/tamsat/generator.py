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

from gap.models import (
    Preferences,
    FarmGroup
)
from core.utils.url_file_checker import file_exists_at_url
from spw.models import SPWExecutionLog, SPWMethod
from spw.tamsat.base import TamsatSPWBase
from spw.tamsat.planting_date_api import (
    routine_operations_v2,
    get_pfc_filename,
    WRSI_FILENAME,
    RoutineDefaults
)

logger = logging.getLogger(__name__)
DEFAULT_MAX_CHUNK_SIZE = 5000


class TamsatSPWGenerator(TamsatSPWBase):
    """SPW Generator for TAMSAT data."""

    SPW_TMP_TABLE_NAME = 'spw_tamsat_tmp'

    def __init__(self, date=None, cleanup=True, verbose=False):
        """Initialize the TamsatSPWGenerator."""
        super().__init__(verbose=verbose)
        self.date = date or timezone.now().date()
        self.working_dir = os.path.join(
            '/tmp',
            f'spw_tamsat_{self.date.isoformat()}'
        )
        os.makedirs(self.working_dir, exist_ok=True)
        self.preferences = Preferences.load()
        self.spw_logs = {}
        self.cleanup = cleanup

    def _init_config(self):
        """Initialize configuration for the SPW generator."""
        super()._init_config()
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

        self.tamsat_url = self.preferences.tamsat_url
        if not self.tamsat_url:
            raise ValueError('TAMSAT URL not found in preferences.')

        self.chunk_size = self.config.get(
            'chunk_size',
            DEFAULT_MAX_CHUNK_SIZE
        )

    def _get_connection(self):
        config = self._get_duckdb_config()
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
        conn.execute(self.CREATE_TABLE_QUERY.format(self.SPW_TMP_TABLE_NAME))
        conn.execute(self.CREATE_TABLE_QUERY.format(self.SPW_TABLE_NAME))

        return conn

    def _insert_farm_group_data(self, conn, df):
        """Insert df to the SPW temporary table."""
        if df.empty:
            logger.warning("No data to insert into SPW temporary table.")
            return

        if 'geometry' not in df.columns:
            df['geometry'] = None

        if 'Latitude' in df.columns or 'Longitude' in df.columns:
            # rename columns to match SPW temporary table
            df.rename(columns={
                'Latitude': 'latitude',
                'Longitude': 'longitude'
            }, inplace=True)

        # reorder columns to match the SPW temporary table
        df = df[[
            'date', 'farm_id', 'farm_unique_id', 'country',
            'farm_group_id', 'farm_group', 'grid_id', 'grid_unique_id',
            'geometry', 'latitude', 'longitude',
            'sm_25', 'sm_50', 'sm_70', 'spw_20',
            'spw_40', 'spw_60', 'pfc_user_probability',
            'wrsi_user_probability', 'pfc_user_decision',
            'wrsi_user_decision', 'sm_user_decision',
            'pfc_thresh', 'pfc_prob_thresh', 'wrsi_thresh_factor',
            'wrsi_prob_thresh'
        ]]

        conn.execute(
            f"""
            INSERT INTO {self.SPW_TMP_TABLE_NAME}
            FROM df
            """
        )

        # log count of inserted rows
        self.count_rows_in_spw_table(conn, self.SPW_TMP_TABLE_NAME)

    def _prepare_geometry(self, conn):
        """Prepare geometry from latitude and longitude columns."""
        conn.execute(f"""
            UPDATE {self.SPW_TMP_TABLE_NAME}
            SET geometry = ST_Point(longitude, latitude)
            WHERE geometry IS NULL;
        """)

    def _move_to_spw_table(self, conn):
        """Move data from temporary table to SPW table."""
        conn.execute(
            f"""
                DELETE FROM {self.SPW_TABLE_NAME}
                WHERE date = '{self.date}'
            """
        )
        conn.execute(
            f"""
                INSERT INTO {self.SPW_TABLE_NAME}
                SELECT * FROM {self.SPW_TMP_TABLE_NAME}
            """
        )
        self.count_rows_in_spw_table(conn, self.SPW_TABLE_NAME)

    def _store_as_geoparquet(self, conn):
        """Store the SPW data as a geoparquet file."""
        # Get boundaries from SPW table
        bbox = conn.execute(
            f"""
            SELECT ST_Extent(geometry) FROM {self.SPW_TABLE_NAME}
            """
        ).fetchone()[0]

        # Delete existing ordered table if it exists
        conn.execute("DROP TABLE IF EXISTS spw_ordered;")

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
                {bbox['min_x']}, {bbox['min_y']},
                {bbox['max_x']}, {bbox['max_y']}
                ))
            );
            """
        )
        conn.execute(sql)

        # Export to GeoParquet
        parquet_path = self._get_current_month_parquet_path(self.date)
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
        """Create chunk slices of farm groups."""
        chunk_slices = []
        for start in range(0, total_count, max_chunk_size):
            end = min(start + max_chunk_size, total_count)
            chunk_slices.append(slice(start, end))
        return chunk_slices

    def _execute_spw_model(
        self, farm_group: FarmGroup, farms_slice, chunk_idx,
        pfc_thresh, pfc_prob_thresh, wrsi_thresh_factor, wrsi_prob_thresh
    ):
        """Execute Tamsat SPW model for a farm group."""
        # Create df with columns:
        # date, farm_id, farm_unique_id, country, farm_group_id,
        # farm_group, grid_id, grid_unique_id, Latitude, Longitude,
        df = pd.DataFrame(columns=[
            'date', 'farm_id', 'farm_unique_id', 'country',
            'farm_group_id', 'farm_group', 'grid_id', 'grid_unique_id',
            'Latitude', 'Longitude', 'pfc_thresh', 'pfc_prob_thresh',
            'wrsi_thresh_factor', 'wrsi_prob_thresh'
        ])

        # Add farm data to df
        for farm in farms_slice:
            new_row = pd.DataFrame({
                'date': [self.date],
                'farm_id': [farm.id],
                'farm_unique_id': [farm.unique_id],
                'country': [
                    farm.grid.country.name if
                    farm.grid and farm.grid.country else None
                ],
                'farm_group_id': [farm_group.id],
                'farm_group': [farm_group.name],
                'grid_id': [farm.grid.id if farm.grid else None],
                'grid_unique_id': [farm.grid.unique_id if farm.grid else None],
                'Latitude': [farm.geometry.y],
                'Longitude': [farm.geometry.x],
                'pfc_thresh': [pfc_thresh],
                'pfc_prob_thresh': [pfc_prob_thresh],
                'wrsi_thresh_factor': [wrsi_thresh_factor],
                'wrsi_prob_thresh': [wrsi_prob_thresh]
            })
            df = pd.concat([df, new_row], ignore_index=True)

        if df.empty:
            logger.warning(
                f"No farms found for farm group: {farm_group.name} "
                f"with chunk index: {chunk_idx}"
            )
            return df

        # export to csv file
        csv_file_path = os.path.join(
            self.working_dir,
            f'spw_tamsat_{farm_group.id}_{chunk_idx}.csv'
        )
        df.to_csv(csv_file_path, index=False)

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
        spw_log = self.spw_logs[farm_group.id]
        pfc_thresh = self.config.get(
            'pfc_thresh', RoutineDefaults.PFC_THRESH
        )
        pfc_prob_thresh = self.config.get(
            'pfc_prob_thresh', RoutineDefaults.PFC_PROB_THRESH
        )
        wrsi_thresh_factor = self.config.get(
            'wrsi_thresh_factor', RoutineDefaults.WRSI_THRESH_FACTOR
        )
        wrsi_prob_thresh = self.config.get(
            'wrsi_prob_thresh', RoutineDefaults.WRSI_PROB_THRESH
        )
        spw_log['log'].config = {
            'pfc_thresh': pfc_thresh,
            'pfc_prob_thresh': pfc_prob_thresh,
            'wrsi_thresh_factor': wrsi_thresh_factor,
            'wrsi_prob_thresh': wrsi_prob_thresh
        }
        spw_log['log'].start()

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
            AND date = '{self.date}';
            """
        )

        # Process farms in chunks
        all_slices = self._get_slices(total_count, self.chunk_size)
        chunk_idx = 0
        for slice_ in all_slices:
            chunk_idx += 1
            spw_log['log'].notes = (
                f"Processing chunk {chunk_idx}/{len(all_slices)} "
                f"for farm group: {farm_group.name}"
            )
            spw_log['log'].save()
            logger.info(
                f"Processing chunk {chunk_idx}/{len(all_slices)} "
                f"for farm group: {farm_group.name}"
            )
            farms_slice = farms[slice_]
            df = self._execute_spw_model(
                farm_group, farms_slice, chunk_idx,
                pfc_thresh, pfc_prob_thresh,
                wrsi_thresh_factor, wrsi_prob_thresh
            )
            if df.empty:
                continue

            self._insert_farm_group_data(conn, df)

        logger.info(
            f"Finished processing farm group: {farm_group.name} "
            f"with {total_count} farms."
        )
        spw_log['log'].success()

    def _cleanup(self):
        """Cleanup temporary files and tables."""
        # Cleanup working directory
        if os.path.exists(self.working_dir):
            logger.info(
                f"Cleaning up working directory: {self.working_dir}"
            )
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
        if self._check_parquet_exists(self.date):
            logger.info(
                "Geoparquet file already exists for the current month. "
                "Pulling existing data."
            )
            self._pull_existing_monthly_data(conn, self.date)

        # Process each farm group
        for farm_group in self.farm_groups:
            self._process_farm_group(conn, farm_group)

        # Prepare geometry
        self._prepare_geometry(conn)

        # debug count of inserted rows
        self.count_rows_in_spw_table(conn, self.SPW_TMP_TABLE_NAME)

        # Move data to SPW table
        self._move_to_spw_table(conn)

        # Store as geoparquet
        self._store_as_geoparquet(conn)

        logger.info("SPW generation completed successfully.")
        conn.close()
        if self.cleanup:
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
