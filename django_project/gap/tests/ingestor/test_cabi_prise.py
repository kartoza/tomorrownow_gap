# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for CABI Prise Ingestor.
"""
import os

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.factories import FarmFactory
from gap.models.ingestor import (
    IngestorSession, IngestorSessionStatus, IngestorType
)
from prise.exceptions import PriseDataTypeNotRecognized
from prise.models.data import PriseData
from prise.models.pest import PrisePest


class CabiPriseIngestorTest(TestCase):
    """Cabi prise ingestor test case."""

    fixtures = [
        '1.object_storage_manager.json',
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '10.pest.json',
        '2.prise_pest.json',
    ]

    def setUp(self) -> None:
        """Set test class."""
        self.farm_1 = FarmFactory(unique_id='FARM1')
        self.farm_2 = FarmFactory(unique_id='FARM2')
        self.farm_100 = FarmFactory(unique_id='FARM100')

    def test_error_no_column_farm_id(self):
        """Test when ingestor error no column for farm id."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'cabi_prise', 'ErrorFieldNotFound.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.CABI_PRISE_EXCEL
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)
        self.assertEqual(
            session.notes,
            "Row 2 does not have 'farmID'"
        )

    def test_error_data_type(self):
        """Test when ingestor has error data type."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'cabi_prise', 'ErrorDataType.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.CABI_PRISE_EXCEL
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)
        self.assertEqual(
            session.notes,
            f'Row 2 : {PriseDataTypeNotRecognized().message}'
        )

    def test_error_farm_not_found(self):
        """Test when ingestor error there is not found farm."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'cabi_prise', 'ErrorExtraFarm.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.CABI_PRISE_EXCEL
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)
        self.assertEqual(
            session.notes,
            'Row 4 : Farm with unique id FARM3 does not exist.'
        )

    def test_error_value_error(self):
        """Test when ingestor error there is value error."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'cabi_prise', 'ErrorValueError.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.CABI_PRISE_EXCEL
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)
        self.assertEqual(
            session.notes,
            'Row 2 : ophiomyia_stat_v02_w1_nrt_midpoint not a float.'
        )

    def test_success(self):
        """Test when ingestor error there is value error."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'cabi_prise', 'Correct.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.CABI_PRISE_EXCEL
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)

        # Farm 1
        data = PriseData.objects.get(
            farm_id=self.farm_1.id
        )
        self.assertEqual(data.prisedatabypest_set.count(), 2)
        self.assertEqual(
            data.prisedatabypest_set.get(
                pest=PrisePest.objects.get(
                    variable_name='ophiomyia_stat_v02_w1_nrt_midpoint'
                ).pest
            ).value,
            39.0
        )
        self.assertEqual(
            data.prisedatabypest_set.get(
                pest=PrisePest.objects.get(
                    variable_name='s_frugiperda_stat_v05_w1_nrt_midpoint'
                ).pest
            ).value,
            50.0
        )

        # Farm 2
        data = PriseData.objects.get(
            farm_id=self.farm_2.id
        )
        self.assertEqual(data.prisedatabypest_set.count(), 2)
        self.assertEqual(
            data.prisedatabypest_set.get(
                pest=PrisePest.objects.get(
                    variable_name='ophiomyia_stat_v02_w1_nrt_midpoint'
                ).pest
            ).value,
            40.0
        )
        self.assertEqual(
            data.prisedatabypest_set.get(
                pest=PrisePest.objects.get(
                    variable_name='s_frugiperda_stat_v05_w1_nrt_midpoint'
                ).pest
            ).value,
            30.0
        )
        # farm 100 should not be ingested
        self.assertEqual(
            PriseData.objects.filter(
                farm_id=self.farm_100.id
            ).count(),
            0
        )
