# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Ingestor.
"""

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.models import (
    IngestorSession, IngestorType, IngestorSessionStatus,
    Attribute, CropGrowthStage, CropStageType, Crop
)
from dcas.models import DCASConfig, DCASRule, DCASMessagePriority
from message.models import MessageTemplate, MessageApplication


class DCASRuleIngestorTest(TestCase):
    """DCAS Rule ingestor test case."""

    fixtures = [
        '1.object_storage_manager.json',
        '6.unit.json',
        '7.attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '1.dcas_config.json'
    ]

    def setUp(self):
        """Set the test class."""
        self.default_config = DCASConfig.objects.get(id=1)

    def test_missing_additional_config(self):
        """Test with missing config."""
        file_path = absolute_path(
            'gap', 'tests', 'ingestor', 'data', 'dcas', 'invalid.csv'
        )
        _file = open(file_path, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.DCAS_RULE,
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)
        self.assertEqual(
            session.notes,
            "rule_config_id is required in additional_config."
        )

    def test_invalid_csv(self):
        """Test with invalid csv."""
        file_path = absolute_path(
            'gap', 'tests', 'ingestor', 'data', 'dcas', 'invalid.csv'
        )
        _file = open(file_path, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.DCAS_RULE,
            additional_config={
                'rule_config_id': self.default_config.id
            },
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)
        self.assertEqual(
            session.notes,
            "Column(s) missing: code"
        )

    def test_success_ingestor(self):
        """Test with success data."""
        # create new config
        config = DCASConfig.objects.create(
            name='test'
        )
        # insert rule into default config
        crop = Crop.objects.create(name='crop1')
        DCASRule.objects.create(
            config=self.default_config,
            crop=crop,
            crop_stage_type=CropStageType.objects.get(name='Early'),
            crop_growth_stage=CropGrowthStage.objects.get(
                name='Vegetative Growth'
            ),
            parameter=Attribute.objects.get(name='P/PET'),
            min_range=10,
            max_range=20,
            code='code_123'
        )

        file_path = absolute_path(
            'gap', 'tests', 'ingestor', 'data', 'dcas', 'valid.csv'
        )
        _file = open(file_path, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.DCAS_RULE,
            additional_config={
                'rule_config_id': config.id
            },
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(
            DCASRule.objects.filter(config=config).count(),
            2
        )
        self.assertEqual(
            DCASRule.objects.filter(config=self.default_config).count(),
            1
        )


class DCASMessageIngestorTest(TestCase):
    """DCAS Message ingestor test case."""

    fixtures = [
        '1.object_storage_manager.json',
        '6.unit.json',
        '7.attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '1.dcas_config.json'
    ]

    def setUp(self):
        """Set the test class."""
        self.default_config = DCASConfig.objects.get(id=1)

    def test_success_ingestor(self):
        """Test with success data."""
        file_path = absolute_path(
            'gap', 'tests', 'ingestor', 'data', 'dcas', 'messages.csv'
        )
        _file = open(file_path, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.DCAS_MESSAGE,
            additional_config={
                'config_id': self.default_config.id
            },
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(
            DCASMessagePriority.objects.filter(
                config=self.default_config
            ).count(),
            50
        )
        self.assertEqual(
            MessageTemplate.objects.filter(
                application=MessageApplication.DCAS
            ).count(),
            50
        )
