# coding: utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCASFarmRegistryIngestor.
"""

import os
import logging
import unittest
import uuid
import shutil
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TransactionTestCase
from django.contrib.gis.geos import GEOSGeometry
from gap.models import (
    Farm, FarmRegistry, FarmRegistryGroup,
    IngestorSession, Grid
)
from gap.ingestor.farm_registry import (
    DCASFarmRegistryIngestor, Keys, FarmRegistryException
)


logger = logging.getLogger(__name__)


class DCASFarmRegistryIngestorTest(TransactionTestCase):
    """Unit tests for DCASFarmRegistryIngestor."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '14.crop.json',
    ]

    def setUp(self):
        """Set up test case."""
        self.working_dir = os.path.join('/tmp', str(uuid.uuid4()))
        os.makedirs(self.working_dir, exist_ok=True)

        grid_wkt = (
            'MULTIPOLYGON (((36.79258221472390034 -1.2866686042944746, '
            '36.82548058282207393 -1.28604788036809392, 36.82666560122697774 '
            '-1.31070754907975973, 36.79303365030672524 -1.31031254294479038, '
            '36.79258221472390034 -1.2866686042944746)))'
        )
        multipolygon = GEOSGeometry(grid_wkt)
        self.grid = Grid.objects.create(
            unique_id='grid001',
            geometry=multipolygon[0],
            elevation=0.0
        )

    def tearDown(self):
        """Tear down test case."""
        shutil.rmtree(self.working_dir)

    def test_successful_ingestion(self):
        """Test successful ingestion of farmer registry data."""
        session = IngestorSession.objects.create(
            ingestor_type='Farm Registry',
            trigger_task=False
        )
        ingestor = DCASFarmRegistryIngestor(session, self.working_dir)

        test_zip_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',  # Test data directory
            'farm_registry',
            'test_farm_registry.zip'  # Pre-existing ZIP file
        )
        with open(test_zip_path, 'rb') as _file:
            test_file = SimpleUploadedFile(_file.name, _file.read())

        # set file in session
        session.file = test_file
        session.save()

        ingestor.run()

        # Verify session status
        session.refresh_from_db()

        # Verify FarmRegistryGroup was created
        self.assertEqual(FarmRegistryGroup.objects.count(), 1)
        group = FarmRegistryGroup.objects.first()
        self.assertTrue(group.is_latest)

        # Verify Farm and FarmRegistry were created
        self.assertEqual(Farm.objects.count(), 2)
        self.assertEqual(FarmRegistry.objects.count(), 2)

        # Verify specific farm details
        farm = Farm.objects.get(unique_id='F001')
        self.assertEqual(farm.geometry.x, 36.8219)
        self.assertEqual(farm.geometry.y, -1.2921)

        # verify temp table has been deleted
        self.assertFalse(ingestor._check_table_exists())

    def test_invalid_lat_lon(self):
        """Test failed ingestion of farmer registry data."""
        session = IngestorSession.objects.create(
            ingestor_type='Farm Registry',
            trigger_task=False
        )
        ingestor = DCASFarmRegistryIngestor(session, self.working_dir)

        test_zip_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',  # Test data directory
            'farm_registry',
            'test_invalid_lat_lon.zip'  # Pre-existing ZIP file
        )
        with open(test_zip_path, 'rb') as _file:
            test_file = SimpleUploadedFile(_file.name, _file.read())

        # set file in session
        session.file = test_file
        session.save()

        with self.assertRaises(FarmRegistryException) as ctx:
            ingestor.run()
        self.assertIn('No rows found in the CSV file.', str(ctx.exception))

        # Verify session status
        session.refresh_from_db()
        self.assertEqual(Farm.objects.count(), 0)
        self.assertEqual(FarmRegistry.objects.count(), 0)

        # verify temp table has been deleted
        self.assertFalse(ingestor._check_table_exists())

    def test_invalid_date_col(self):
        """Test failed ingestion of farmer registry data."""
        session = IngestorSession.objects.create(
            ingestor_type='Farm Registry',
            trigger_task=False
        )
        ingestor = DCASFarmRegistryIngestor(session, self.working_dir)

        test_zip_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',  # Test data directory
            'farm_registry',
            'test_invalid_date.zip'  # Pre-existing ZIP file
        )
        with open(test_zip_path, 'rb') as _file:
            test_file = SimpleUploadedFile(_file.name, _file.read())

        # set file in session
        session.file = test_file
        session.save()

        with self.assertRaises(FarmRegistryException) as ctx:
            ingestor.run()
        self.assertIn('Mismatch in row counts', str(ctx.exception))

        # Verify session status
        session.refresh_from_db()
        self.assertEqual(Farm.objects.count(), 2)
        self.assertEqual(FarmRegistry.objects.count(), 0)

        # verify temp table has been deleted
        self.assertFalse(ingestor._check_table_exists())


class TestKeysStaticMethods(unittest.TestCase):
    """Test static methods in Keys class."""

    def test_get_crop_key(self):
        """Test get_crop_key."""
        self.assertEqual(
            Keys.get_crop_key({'CropName': 'Maize'}), 'CropName')
        self.assertEqual(
            Keys.get_crop_key({'crop': 'Cassava'}), 'crop')
        with self.assertRaises(KeyError):
            Keys.get_crop_key({'wrong_key': 'Soybean'})

    def test_get_planting_date_key(self):
        """Test get_planting_date_key."""
        self.assertEqual(
            Keys.get_planting_date_key(
                {'PlantingDate': '2024-01-01'}), 'PlantingDate')
        self.assertEqual(
            Keys.get_planting_date_key(
                {'plantingDate': '2024-01-01'}), 'plantingDate')
        with self.assertRaises(KeyError):
            Keys.get_planting_date_key({'date': '2024-01-01'})

    def test_get_farm_id_key(self):
        """Test get_farm_id_key."""
        self.assertEqual(
            Keys.get_farm_id_key({'FarmerId': '123'}), 'FarmerId')
        self.assertEqual(
            Keys.get_farm_id_key({'farmer_id': '456'}), 'farmer_id')
        with self.assertRaises(KeyError):
            Keys.get_farm_id_key({'id': '789'})
