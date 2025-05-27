# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Ingestor.
"""
import os

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.factories import FarmGroupFactory
from gap.ingestor.exceptions import (
    FileNotFoundException, AdditionalConfigNotFoundException
)
from gap.ingestor.farm import Keys, Farm
from gap.models.ingestor import (
    IngestorSession, IngestorSessionStatus, IngestorType
)


class FarmIngestorTest(TestCase):
    """Farm ingestor test case."""

    fixtures = [
        '1.object_storage_manager.json',
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self) -> None:
        """Set test class."""
        self.farm_group = FarmGroupFactory()

    def test_error_no_configuration(self):
        """Test when ingestor error no farm_group_id."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'ErrorColumn.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes,
            AdditionalConfigNotFoundException('farm_group_id').message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_farm_group_not_found(self):
        """Test when ingestor error does not find farm_group_id."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'ErrorColumn.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': 0
            }
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, 'Farm group does not exist'
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_no_file(self):
        """Test no file ingestor."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': self.farm_group.id
            }
        )
        session.run()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_column(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'ErrorColumn.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': self.farm_group.id
            }
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, f"Row 3 does not have '{Keys.GEOMETRY}'"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_coordinate(self):
        """Test when ingestor error coordinate is error."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'ErrorCoordinate.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': self.farm_group.id
            }
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, "Row 3 : Invalid latitude, longitude format"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_working(self):
        """Test when ingestor working fine."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'Correct.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': self.farm_group.id
            }
        )
        session.run()
        self.assertEqual(
            session.ingestorsessionprogress_set.filter(
                session=session,
                status=IngestorSessionStatus.FAILED
            ).count(), 0
        )
        session.delete()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        farms = Farm.objects.all()
        self.assertEqual(farms.count(), 3)
        self.assertEqual(farms[0].unique_id, '1001')
        self.assertEqual(farms[0].phone_number, '123-456-7890')
        self.assertEqual(farms[0].rsvp_status.name, 'Accepted')
        self.assertEqual(farms[0].category.name, 'Treatment')
        self.assertEqual(farms[0].village.name, 'Village A')
        self.assertEqual(farms[0].crop.name, 'Maize')
        self.assertEqual(farms[0].geometry.y, -0.2991111111111111)
        self.assertEqual(farms[0].geometry.x, 35.88930555555555)

        self.assertEqual(farms[1].unique_id, '1002')
        self.assertEqual(farms[1].phone_number, '123-456-7890')
        self.assertEqual(farms[1].rsvp_status.name, 'Declined')
        self.assertEqual(farms[1].category.name, 'Treatment')
        self.assertEqual(farms[1].village.name, 'Village B')
        self.assertEqual(farms[1].crop.name, 'Common Beans')
        self.assertEqual(farms[1].geometry.y, -0.29894444444444446)
        self.assertEqual(farms[1].geometry.x, 35.890972222222224)

        self.assertEqual(farms[2].unique_id, '1003')
        self.assertEqual(farms[2].phone_number, '0123456')
        self.assertEqual(farms[2].rsvp_status.name, 'Declined')
        self.assertEqual(farms[2].category.name, 'Control')
        self.assertEqual(farms[2].village.name, 'Village C')
        self.assertEqual(farms[2].crop.name, 'Wheat')
        self.assertEqual(farms[2].geometry.y, -0.2940277777777778)
        self.assertEqual(farms[2].geometry.x, 35.885)

        # Farm in correct group
        for farm in farms:
            self.farm_group.farms.get(id=farm.id)

    def test_run_duplicates(self):
        """Test when ingestor working with duplicates."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'Correct.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': self.farm_group.id
            },
            trigger_task=False
        )
        session.run()
        self.assertEqual(
            session.ingestorsessionprogress_set.filter(
                session=session,
                status=IngestorSessionStatus.FAILED
            ).count(), 0
        )
        session.delete()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        farms = Farm.objects.all()
        self.assertEqual(farms.count(), 3)
        # re-run and get the duplicates
        _file.close()
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM,
            additional_config={
                'farm_group_id': self.farm_group.id,
                'skip_existing_farm_id': True
            },
            trigger_task=False
        )
        session.run()
        self.assertEqual(
            session.ingestorsessionprogress_set.filter(
                session=session,
                status=IngestorSessionStatus.FAILED
            ).count(), 0
        )
        self.assertIn('duplicate_ids_path', session.additional_config)
        self.assertIn('duplicate_ids_count', session.additional_config)
        self.assertEqual(
            session.additional_config['duplicate_ids_count'], 3
        )
