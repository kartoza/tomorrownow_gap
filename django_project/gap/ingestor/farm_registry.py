# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor for DCAS Farmer Registry data
"""

import os
import tempfile
import zipfile
import subprocess
import time
from datetime import datetime, timezone
from django.db import connection
import logging
from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException,
)
from gap.models import (
    FarmRegistryGroup,
    IngestorSession,
    IngestorSessionStatus
)

logger = logging.getLogger(__name__)


class Keys:
    """Keys for the data."""

    CROP = 'CropName'
    ALT_CROP = 'crop'
    FARMER_ID = 'FarmerId'
    ALT_FARMER_ID = 'farmer_id'
    FINAL_LATITUDE = 'FinalLatitude'
    FINAL_LONGITUDE = 'FinalLongitude'
    PLANTING_DATE = 'PlantingDate'
    ALT_PLANTING_DATE = 'plantingDate'

    @staticmethod
    def check_columns(df) -> bool:
        """Check if all columns exist in dataframe.

        :param df: dataframe from csv
        :type df: pd.DataFrame
        :raises FileIsNotCorrectException: When column is missing
        """
        keys = [
            Keys.CROP, Keys.FARMER_ID, Keys.FINAL_LATITUDE,
            Keys.FINAL_LONGITUDE, Keys.PLANTING_DATE
        ]

        missing = []
        for key in keys:
            if key not in df.columns:
                missing.append(key)

        if missing:
            raise FileIsNotCorrectException(
                f'Column(s) missing: {",".join(missing)}'
            )

    @staticmethod
    def get_crop_key(row):
        """Handle both 'CropName' and 'crop' key variations."""
        if Keys.CROP in row:
            return Keys.CROP
        elif Keys.ALT_CROP in row:
            return Keys.ALT_CROP
        else:
            raise KeyError(f"No valid crop key found in row: {row}")

    @staticmethod
    def get_planting_date_key(row):
        """Handle both 'PlantingDate' and 'plantingDate' key variations."""
        if Keys.PLANTING_DATE in row:
            return Keys.PLANTING_DATE
        elif Keys.ALT_PLANTING_DATE in row:
            return Keys.ALT_PLANTING_DATE
        else:
            raise KeyError(f"No valid planting date key found in row: {row}")

    @staticmethod
    def get_farm_id_key(row):
        """Handle both 'FarmerId' and 'farmer_id' key variations."""
        if Keys.FARMER_ID in row:
            return Keys.FARMER_ID
        elif Keys.ALT_FARMER_ID in row:
            return Keys.ALT_FARMER_ID
        else:
            raise KeyError(f"No valid farmer ID key found in row: {row}")


class FarmRegistryException(Exception):
    """Custom exception for FarmRegistry."""

    pass


class DCASFarmRegistryIngestor(BaseIngestor):
    """Ingestor for DCAS Farmer Registry data."""

    def __init__(self, session: IngestorSession, working_dir='/tmp'):
        """Initialize the ingestor with session and working directory.

        :param session: Ingestor session object
        :type session: IngestorSession
        :param working_dir: Directory to extract ZIP files temporarily
        :type working_dir: str, optional
        """
        super().__init__(session, working_dir)

        # Placeholder for the group created during this session
        self.group = None
        self.execution_times = {}
        self.table_name = f'temp.farm_registry_session_{self.session.id}'
        self.table_name_sql = (
            f'"temp"."farm_registry_session_{self.session.id}"'
        )

    def _extract_zip_file(self):
        """Extract the ZIP file to a temporary directory."""
        with self.session.file.open('rb') as zip_file:
            with tempfile.NamedTemporaryFile(
                delete=False, dir=self.working_dir) as tmp_file:

                tmp_file.write(zip_file.read())
                tmp_file_path = tmp_file.name

        with zipfile.ZipFile(tmp_file_path, 'r') as zip_ref:
            zip_ref.extractall(self.working_dir)

        os.remove(tmp_file_path)

    def _create_registry_group(self):
        """Create a new FarmRegistryGroup."""
        group_id = self.get_config('group_id', None)
        if group_id:
            self.group = FarmRegistryGroup.objects.get(id=group_id)
            return

        group_name = "farm_registry_" + \
            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
        self.group = FarmRegistryGroup.objects.create(
            name=group_name,
            date_time=datetime.now(timezone.utc),
            is_latest=True
        )

    def _execute_query(self, query, query_name):
        """Execute a query on the database."""
        progress = self._add_progress(query_name)
        start_time = time.time()
        with connection.cursor() as cursor:
            cursor.execute(query)
        total_time = time.time() - start_time
        self.execution_times[query_name] = total_time
        progress.notes = f"Execution time: {total_time}"
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

    def _check_table_exists(self):
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'temp'
                    AND table_name = 'farm_registry_session_{self.session.id}'
                );
            """)
            table_exists = cursor.fetchone()[0]
        return table_exists

    def _drop_table(self):
        """Drop the temporary table if it exists."""
        # remove temporary table
        self._execute_query(
            f'DROP TABLE IF EXISTS {self.table_name_sql}',
            'drop_temp_table'
        )

    def _create_table(self):
        self._drop_table()
        # create temporary table
        self._execute_query(
            f"""
            CREATE unlogged TABLE {self.table_name_sql} (
                ogc_fid serial4 NOT NULL,
                farmer_id varchar NULL,
                crop varchar NULL,
                planting_date date NULL,
                grid_id int4 NULL,
                farm_id int4 NULL,
                crop_txt varchar NULL,
                crop_stage_txt varchar NULL,
                crop_id int4 NULL,
                crop_stage_id int4 NULL,
                farm_registry_id int4 NULL,
                county varchar NULL,
                county_id int4 NULL,
                subcounty varchar NULL,
                subcounty_id int4 NULL,
                ward varchar NULL,
                ward_id int4 NULL,
                language varchar NULL,
                language_id int4 NULL,
                wkb_geometry public.geometry(point, 4326) NULL,
                CONSTRAINT farm_registry_session_{self.session.id}_pkey
                PRIMARY KEY (ogc_fid)
            );
            """,
            'create_temp_table'
        )

    def _merge_lookup_field(self, field_name, table_name, ref_name):
        """Merge join to fill county_id."""
        # update county_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gc.id as {field_name}, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN {table_name} gc ON
                    LOWER(gc.name) = LOWER(tfrs.{ref_name})
            )
            UPDATE {self.table_name_sql} tfrs
            SET {field_name} = m.{field_name}
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, f'update_{field_name}')

        # insert into gap_county for missing counties
        self._execute_query(f"""
            INSERT INTO {table_name} (name)
            SELECT DISTINCT ON (tfrs.{ref_name}) tfrs.{ref_name}
            FROM {self.table_name_sql} tfrs
            WHERE tfrs.{field_name} IS NULL AND tfrs.{ref_name} IS NOT NULL;
        """, f'insert_{field_name}')

        # update back the county_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gc.id as {field_name}, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN {table_name} gc ON
                    LOWER(gc.name) = LOWER(tfrs.{ref_name})
                WHERE tfrs.{field_name} IS NULL
            )
            UPDATE {self.table_name_sql} tfrs
            SET {field_name} = m.{field_name}
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, f'update_{field_name}_2')

    def _run(self):
        """Run the ingestion logic."""
        dir_path = self.working_dir
        self._create_registry_group()
        logger.debug(f"Created new registry group: {self.group.id}")

        file_path = None
        for file_name in os.listdir(dir_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(dir_path, file_name)
                break

        if file_path is None:
            raise FarmRegistryException(
                'No CSV file found in the extracted ZIP.'
            )

        # rename csv file
        new_file_path = os.path.join(
            dir_path, f'farm_{self.session.id}.csv'
        )
        os.rename(file_path, new_file_path)
        # Ensure the CSV file has read permissions
        os.chmod(new_file_path, 0o644)
        file_path = new_file_path
        layer_name = os.path.basename(file_path).replace('.csv', '')

        # create vrt file
        vrt_content = (
            f"""<OGRVRTDataSource>
                <OGRVRTLayer name="{layer_name}">
                    <SrcDataSource relativeToVRT="1">
                    {os.path.basename(file_path)}</SrcDataSource>
                    <GeometryType>wkbPoint</GeometryType>
                    <LayerSRS>EPSG:4326</LayerSRS>
                    <GeometryField encoding="PointFromColumns"
                        x="FinalLongitude" y="FinalLatitude"/>
                    <Field name="farmer_id" type="String"/>
                    <Field name="crop" type="String"/>
                    <Field name="planting_date"
                        src="plantingDate" type="Date"/>
                    <Field name="county" type="String"/>
                    <Field name="subcounty" type="String"/>
                    <Field name="ward" type="String"/>
                    <Field name="language" type="String"/>
                    <Field name="grid_id" type="Integer"/>
                    <Field name="farm_id" type="Integer"/>
                    <Field name="crop_txt" type="String"/>
                    <Field name="crop_stage_txt" type="String"/>
                    <Field name="crop_id" type="Integer"/>
                    <Field name="crop_stage_id" type="Integer"/>
                    <Field name="farm_registry_id" type="Integer"/>
                    <Field name="county_id" type="Integer"/>
                    <Field name="subcounty_id" type="Integer"/>
                    <Field name="ward_id" type="Integer"/>
                    <Field name="language_id" type="Integer"/>
                </OGRVRTLayer>
            </OGRVRTDataSource>"""
        )
        vrt_file_path = os.path.join(dir_path, f'{layer_name}.vrt')
        with open(vrt_file_path, 'w') as vrt_file:
            vrt_file.write(vrt_content)
        os.chmod(vrt_file_path, 0o644)

        # create temp schema if not exist
        with connection.cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS temp;')

        # create temporary table
        self._create_table()

        # run ogr2ogr command
        progress = self._add_progress('ogr2ogr')
        ogr2_start_time = time.time()
        conn_str = (
            'PG:dbname={NAME} user={USER} password={PASSWORD} '
            'host={HOST} port={PORT}'.format(
                **connection.settings_dict
            )
        )
        cmd_list = [
            'ogr2ogr',
            '-f',
            'PostgreSQL',
            conn_str,
            vrt_file_path,
            '-nln',
            self.table_name,
            '-append',
            '--config',
            'PG_USE_COPY',
            'YES'
        ]
        subprocess.run(cmd_list, check=True)
        ogr2_total_time = time.time() - ogr2_start_time
        self.execution_times['ogr2ogr'] = ogr2_total_time

        # Check if the table exists in the database
        table_exists = self._check_table_exists()

        if not table_exists:
            progress.notes = (
                f"Table farm_registry_session_{self.session.id} "
                "is failed to be created!"
            )
            progress.status = IngestorSessionStatus.FAILED
            progress.save()
            raise FarmRegistryException(
                f"Temporary table {self.table_name} does not exist."
            )
        # Get total row count in the temporary table
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name_sql};")
            self.execution_times['total_rows'] = cursor.fetchone()[0]
            progress.row_count = self.execution_times['total_rows']

        if progress.row_count == 0:
            progress.notes = "No rows found in the CSV file."
            progress.status = IngestorSessionStatus.FAILED
            progress.save()
            raise FarmRegistryException(
                "No rows found in the CSV file."
            )

        progress.notes = f"Execution time: {ogr2_total_time}"
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

        # create gist index
        self._execute_query(
            f"""
            CREATE INDEX
            frs_{self.session.id}_wkb_geometry_geom_idx ON
            {self.table_name_sql} USING gist (wkb_geometry);
            """,
            'create_gist_index'
        )

        # create index on columns: farmer_id, crop_txt, crop_stage_txt
        # and other text columns
        progress = self._add_progress('create_index')
        with connection.cursor() as cursor:
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (farmer_id)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (crop_txt)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (crop_stage_txt)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (county)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (subcounty)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (ward)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} (language)'
            )
            cursor.execute(
                f'CREATE INDEX ON {self.table_name_sql} '
                '(farm_id, crop_id, crop_stage_id, planting_date)'
            )
        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()

        # update farm_id using join on unique_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gf.unique_id, gf.id as farm_id, gf.grid_id,
                tfrs.farmer_id, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN gap_farm gf ON gf.unique_id = tfrs.farmer_id
            )
            UPDATE {self.table_name_sql} tfrs
            SET farm_id = m.farm_id, grid_id = m.grid_id
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, 'update_farm_id')

        # update grid_id using spatial join
        self._execute_query(f"""
            WITH matched AS (
                SELECT tfrs.ogc_fid, g.id as grid_id
                FROM {self.table_name_sql} tfrs
                JOIN gap_grid g
                ON ST_Intersects(g.geometry, tfrs.wkb_geometry)
                WHERE tfrs.grid_id IS NULL
            )
            UPDATE {self.table_name_sql} tfrs
            SET grid_id = m.grid_id
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, 'update_grid_id')

        # split crop into crop_txt and crop_stage_txt
        self._execute_query(f"""
            UPDATE {self.table_name_sql}
            SET crop_txt = split_part(crop, '_', 1),
                crop_stage_txt = split_part(crop, '_', 2);
        """, 'split_crop')

        # fix Medium stage
        self._execute_query(f"""
            UPDATE {self.table_name_sql}
            SET crop_stage_txt = 'Mid'
            WHERE crop_stage_txt = 'Medium';
        """, 'fix_medium_stage')

        # update crop_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gc.id as crop_id, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN gap_crop gc ON LOWER(gc.name) = LOWER(tfrs.crop_txt)
            )
            UPDATE {self.table_name_sql} tfrs
            SET crop_id = m.crop_id
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, 'update_crop_id')

        # update crop_stage_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gc.id as stage_id, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN gap_cropstagetype gc ON
                LOWER(gc.name) = LOWER(tfrs.crop_stage_txt)
            )
            UPDATE {self.table_name_sql} tfrs
            SET crop_stage_id = m.stage_id
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, 'update_crop_stage_id')

        # merge county
        self._merge_lookup_field(
            'county_id', 'gap_county', 'county'
        )
        # merge subcounty
        self._merge_lookup_field(
            'subcounty_id', 'gap_subcounty', 'subcounty'
        )
        # merge ward
        self._merge_lookup_field(
            'ward_id', 'gap_ward', 'ward'
        )
        # merge language (using name)
        self._merge_lookup_field(
            'language_id', 'gap_language', 'language'
        )

        # insert into gap_farm
        self._execute_query(f"""
            INSERT INTO public.gap_farm (unique_id, geometry, grid_id)
            SELECT DISTINCT ON (tfrs.farmer_id)
            tfrs.farmer_id, tfrs.wkb_geometry, tfrs.grid_id
            FROM {self.table_name_sql} tfrs
            WHERE tfrs.farm_id IS NULL AND tfrs.grid_id IS NOT NULL;
        """, 'insert_farm')

        # update back the farm_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gf.unique_id, gf.id as farm_id,
                tfrs.farmer_id, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN gap_farm gf ON gf.unique_id = tfrs.farmer_id
                WHERE tfrs.farm_id IS NULL
            )
            UPDATE {self.table_name_sql} tfrs
            SET farm_id = m.farm_id
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, 'update_farm_id_2')

        # update existing farm_registry_id
        self._execute_query(f"""
            WITH matched AS (
                SELECT gf.id as existing_id, tfrs.ogc_fid
                FROM {self.table_name_sql} tfrs
                JOIN gap_farmregistry gf ON
                gf.farm_id = tfrs.farm_id AND
                gf.crop_id = tfrs.crop_id AND
                gf.crop_stage_type_id = tfrs.crop_stage_id AND
                gf.planting_date = tfrs.planting_date
                WHERE gf.group_id = {self.group.id}
            )
            UPDATE {self.table_name_sql} tfrs
            SET farm_registry_id = m.existing_id
            FROM matched m
            WHERE tfrs.ogc_fid = m.ogc_fid;
        """, 'update_farm_registry_id')

        # Count rows that will be inserted into gap_farmregistry
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {self.table_name_sql} tfrs
                WHERE tfrs.farm_id IS NOT NULL AND
                tfrs.crop_id IS NOT NULL AND
                tfrs.crop_stage_id IS NOT NULL AND
                tfrs.planting_date IS NOT NULL AND
                tfrs.grid_id IS NOT NULL AND
                tfrs.farm_registry_id IS NULL;
            """)
            self.execution_times['farmregistry_insert_count'] = (
                cursor.fetchone()[0]
            )

        # Check if farmregistry_insert_count matches total_rows
        if (
            self.execution_times['farmregistry_insert_count'] !=
            self.execution_times['total_rows']
        ):
            self._add_progress(
                'mismatch_row_count',
                "Mismatch in row counts: "
                "farmregistry_insert_count "
                f"({self.execution_times['farmregistry_insert_count']}) "
                "does not match total_rows "
                f"({self.execution_times['total_rows']})."
            )

        # Check existing farmregistry_id
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {self.table_name_sql} tfrs
                WHERE tfrs.farm_registry_id IS NOT NULL;
            """)
            self.execution_times['existing_farmregistry_count'] = (
                cursor.fetchone()[0]
            )
        if self.execution_times['existing_farmregistry_count'] > 0:
            self._add_progress(
                'existing_farmregistry_count',
                f"Existing farm_registry_id count: "
                f"{self.execution_times['existing_farmregistry_count']}"
            )

        # insert into gap_farmregistry
        self._execute_query(f"""
            INSERT INTO public.gap_farmregistry
            (planting_date, crop_id, crop_stage_type_id, farm_id, group_id,
            county_id, subcounty_id, ward_id, language_id)
            SELECT tfrs.planting_date, tfrs.crop_id,
            tfrs.crop_stage_id, tfrs.farm_id, {self.group.id} as group_id,
            tfrs.county_id, tfrs.subcounty_id,
            tfrs.ward_id, tfrs.language_id
            FROM {self.table_name_sql} tfrs
            WHERE tfrs.farm_id IS NOT NULL AND tfrs.crop_id IS NOT NULL AND
            tfrs.crop_stage_id IS NOT NULL AND
            tfrs.planting_date IS NOT NULL AND tfrs.grid_id IS NOT NULL AND
            tfrs.farm_registry_id IS NULL;
        """, 'insert_farmregistry')

    def run(self):
        """Run the ingestion process."""
        if not self.session.file:
            raise FileNotFoundException("No file found for ingestion.")
        try:
            self._extract_zip_file()
            self._run()
        except Exception as e:
            raise e
        finally:
            # remove temporary table
            self._drop_table()

            # store execution times in session
            if self.session.additional_config:
                self.session.additional_config.update(self.execution_times)
            else:
                self.session.additional_config = self.execution_times

            # store the farm registry group id
            self.session.additional_config['farm_registry_group_id'] = (
                self.group.id
            )
