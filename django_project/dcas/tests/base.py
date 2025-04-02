# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base test for DCAS Pipeline.
"""

import datetime
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.models import IngestorSession, IngestorType, Crop, CropStageType
from gap.factories import (
    FarmRegistryGroupFactory, FarmRegistryFactory,
    FarmFactory, GridFactory, CountryFactory
)
from dcas.models import DCASConfig


class BaseRuleEngineTest:
    """Base Test class for Rule Engine."""

    def _ingest_rule(self):
        """Ingest ruleset."""
        default_config = DCASConfig.objects.get(id=1)
        file_path = absolute_path(
            'gap', 'tests', 'ingestor', 'data', 'dcas', 'valid.csv'
        )
        _file = open(file_path, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.DCAS_RULE,
            additional_config={
                'rule_config_id': default_config.id
            },
            trigger_task=False
        )
        session.run()
        session.delete()


class BasePipelineTest(BaseRuleEngineTest):
    """Base pipeline test."""

    def setup_test(self):
        """Set the test class."""
        country_1 = CountryFactory(
            iso_a3='KE'
        )
        country_2 = CountryFactory(
            iso_a3='ZAF'
        )
        self.grid_1 = GridFactory(
            country=country_1
        )
        self.grid_2 = GridFactory(
            country=country_2
        )

        self.default_config = DCASConfig.objects.get(id=1)
        self.request_date = datetime.date(2025, 1, 15)

        # create farm registries
        self.farm_registry_group = FarmRegistryGroupFactory()
        self.farm_registry_1 = self._create_farm_registry_for_grid(
            self.farm_registry_group, self.grid_1,
            Crop.objects.get(name='Cassava'),
            CropStageType.objects.get(name='Early')
        )
        self.farm_registry_2 = self._create_farm_registry_for_grid(
            self.farm_registry_group, self.grid_2,
            Crop.objects.get(name='Cassava'),
            CropStageType.objects.get(name='Early')
        )

    def _create_farm_registry_for_grid(
        self, group, grid, crop, crop_stage_type
    ):
        farm = FarmFactory(grid=grid)
        return FarmRegistryFactory(
            group=group,
            farm=farm,
            crop=crop,
            crop_stage_type=crop_stage_type,
            planting_date=datetime.date(2025, 1, 9)
        )


class DCASPipelineBaseTest(TestCase, BasePipelineTest):
    """DCAS Pipeline base test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '1.dcas_config.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '14.crop.json',
        '15.gdd_config.json',
        '16.gdd_matrix.json'
    ]

    def setUp(self):
        """Set DCASPipelineBaseTest class."""
        self.setup_test()
