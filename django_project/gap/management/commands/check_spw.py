from django.core.management.base import BaseCommand
import duckdb
from django.db import connection

from gap.models import Preferences, FarmGroup
from gap.models.ingestor import IngestorSession, IngestorType
from gap.ingestor.spw import SPWIngestor


class Command(BaseCommand):
    help = 'Prints Hello, World! to the console'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Hello, World!'))
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 11,
                'year': 2024
            }
        )
        print(f'Session created with ID: {session.id}')
        farm_group = FarmGroup.objects.get(
            name='Regen organics pilot'
        )
        preferences = Preferences.load()
        preferences.crop_plan_config['geoparquet_path'] = 'spw_geoparquet_check'
        preferences.save()

        ingestor = SPWIngestor(session)
        ingestor._init_config()

        # init duckdb connection
        conn = ingestor._get_connection(ingestor.s3)
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
        ingestor._execute_sql(
            conn, create_table_query,
            'Creating spw_signal table if not exists'
        )

        ingestor._insert_by_farm_group(conn, farm_group)
        count_sql = (
            """
            SELECT COUNT(*) FROM spw_signal
            """
        )
        conn.sql(count_sql).show()
        conn.sql(
            """
            SELECT farm_unique_id, geometry FROM spw_signal
            LIMIT 10
            """
        ).show()
