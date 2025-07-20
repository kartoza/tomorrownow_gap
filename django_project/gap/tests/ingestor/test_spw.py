# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for SPW Ingestor.
"""

from django.test import TransactionTestCase
from django.contrib.gis.geos import Point

from gap.models import (
    IngestorSession, IngestorType,
    FarmGroup, Preferences
)
from gap.ingestor.exceptions import (
    AdditionalConfigNotFoundException,
    NoDataException
)
from gap.ingestor.spw import SPWIngestor
from gap.factories import (
    FarmSuitablePlantingWindowSignalFactory,
    FarmFactory
)


class SPWIngestorTest(TransactionTestCase):
    """SPW ingestor test case."""

    fixtures = [
        '1.object_storage_manager.json',
        '11.farm_group.json',
    ]

    def setUp(self):
        """Init test case."""
        self.farm_group1 = FarmGroup.objects.get(
            name="Regen organics pilot"
        )
        self.farm_group2 = FarmGroup.objects.get(
            name="Trial site 1"
        )
        self.preferences = Preferences.load()
        self.preferences.crop_plan_config = {
            'geoparquet_path': 'spw_geoparquet_test'
        }
        self.preferences.save()
        self.farm1 = FarmFactory.create(
            geometry=Point(0, 0)
        )
        self.farm2 = FarmFactory.create(
            geometry=Point(0, 1)
        )
        self.farm3 = FarmFactory.create(
            geometry=Point(1, 1)
        )
        self.farm4 = FarmFactory.create(
            geometry=Point(1, 0)
        )
        self.farm_group1.farms.add(self.farm1, self.farm2)
        self.farm_group2.farms.add(self.farm3, self.farm4)
        self.farm_group1.save()
        self.farm_group2.save()

    def test_init_config(self):
        """Test SPW ingestor configuration."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False
        )
        ingestor = SPWIngestor(session)
        with self.assertRaises(AdditionalConfigNotFoundException) as ctx:
            ingestor._init_config()
        self.assertEqual(
            str(ctx.exception),
            'Month and year is required in additional_config.'
        )

        self.preferences.crop_plan_config = {}
        self.preferences.save()
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        with self.assertRaises(AdditionalConfigNotFoundException) as ctx:
            ingestor._init_config()
        self.assertEqual(
            str(ctx.exception),
            'Geoparquet path (crop_plan_config) is required '
            'in additional_config.'
        )

        self.preferences.crop_plan_config = {
            'geoparquet_path': 'spw_geoparquet_test'
        }
        self.preferences.save()
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        ingestor._init_config()
        self.assertEqual(
            ingestor.geoparquet_path, 'spw_geoparquet_test'
        )
        self.assertEqual(
            ingestor.geoparquet_connection_name, 'default'
        )
        self.assertEqual(
            ingestor.month, 1
        )
        self.assertEqual(
            ingestor.year, 2023
        )

    def test_get_connection(self):
        """Test getting connection."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        ingestor.use_ssl = False
        ingestor._init_config()
        conn = ingestor._get_connection(ingestor.s3)
        self.assertIsNotNone(conn)
        extensions = conn.sql(
            "SELECT extension_name FROM duckdb_extensions() "
            "where loaded=true"
        ).fetchall()
        extensions = [ext[0] for ext in extensions]
        self.assertIn('httpfs', extensions)
        self.assertIn('spatial', extensions)
        self.assertIn('postgres_scanner', extensions)
        conn.close()

    def test_get_farms_boundaries(self):
        """Test getting farms boundaries."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        ingestor._init_config()
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm1,
            generated_date='2023-01-01'
        )
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm2,
            generated_date='2023-01-01'
        )
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm3,
            generated_date='2023-01-01'
        )
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm4,
            generated_date='2023-01-01'
        )
        bbox = ingestor._get_farms_boundaries()
        self.assertEqual(bbox[0], 0)
        self.assertEqual(bbox[1], 0)
        self.assertEqual(bbox[2], 1)
        self.assertEqual(bbox[3], 1)

    def test_get_parquet_path(self):
        """Test getting parquet path."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        ingestor._init_config()
        path = ingestor._get_parquet_path()
        expected_path = (
            f"s3://{ingestor.s3['S3_BUCKET_NAME']}/"
            f"{ingestor.s3['S3_DIR_PREFIX']}/spw_geoparquet_test/"
            f"year=2023/month=1.parquet"
        )
        self.assertEqual(path, expected_path)

    def test_run_empty(self):
        """Test running SPW ingestor with no data."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        with self.assertRaises(NoDataException) as ctx:
            ingestor._run()
        self.assertEqual(
            str(ctx.exception),
            'No data found for SPW on 1-2023.'
        )

    def test_run(self):
        """Test running SPW ingestor with data."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SPW_GEOPARQUET,
            trigger_task=False,
            additional_config={
                'month': 1,
                'year': 2023
            }
        )
        ingestor = SPWIngestor(session)
        ingestor.use_ssl = False
        ingestor._init_config()
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm1,
            generated_date='2023-01-01'
        )
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm2,
            generated_date='2023-01-01'
        )
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm3,
            generated_date='2023-01-01'
        )
        FarmSuitablePlantingWindowSignalFactory.create(
            farm=self.farm4,
            generated_date='2023-01-01'
        )

        ingestor._run()
        # Check if the parquet file is created
        path = ingestor._get_parquet_path()
        conn = ingestor._get_connection(ingestor.s3)
        count = conn.sql(
            f"SELECT COUNT(*) FROM read_parquet('{path}')"
        ).fetchone()[0]
        self.assertTrue(count > 0)
        conn.close()
