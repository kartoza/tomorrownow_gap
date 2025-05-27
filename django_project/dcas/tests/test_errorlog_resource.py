# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCASErrorLogResource.
"""

import datetime
from django.test import TestCase
from django.contrib.gis.geos import Point
from tablib import Dataset
from dcas.models import DCASErrorLog, DCASErrorType, DCASRequest
from dcas.resources import DCASErrorLogResource
from gap.models import (
    Farm, FarmRegistry, Crop, CropStageType,
    Country
)
from gap.factories import FarmRegistryGroupFactory


class TestDCASErrorLogResource(TestCase):
    """Test case for DCASErrorLogResource."""

    fixtures = [
        '1.object_storage_manager.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '14.crop.json'
    ]

    def setUp(self):
        """Set up test data."""
        self.country = Country.objects.create(
            name="Test Country",
            iso_a3="TST"
        )
        self.farm_registry_group = FarmRegistryGroupFactory()
        self.farm = Farm.objects.create(
            unique_id="FARM123",
            geometry=Point(0.0, 0.0)
        )
        self.farm_registry = FarmRegistry.objects.create(
            group=self.farm_registry_group,
            farm=self.farm,
            crop=Crop.objects.get(name='Cassava'),
            crop_stage_type=CropStageType.objects.get(name='Early'),
            planting_date=datetime.date(2025, 1, 9)
        )
        self.request = DCASRequest.objects.create(
            requested_at="2024-01-01T00:00:00Z",
            country=self.country
        )

        self.error_log = DCASErrorLog.objects.create(
            request=self.request,
            farm_registry=self.farm_registry,
            error_type=DCASErrorType.MISSING_MESSAGES,
            error_message="Test missing message error"
        )

        self.resource = DCASErrorLogResource()

    def test_resource_fields(self):
        """Ensure the resource includes correct fields."""
        expected_fields = {
            "id", "Request ID", "Farm ID",
            "Error Type", "Error Message", "Logged At"
        }
        actual_fields = {
            field.column_name for field in self.resource.get_export_fields()
        }
        expected_fields = {
            "Request ID", "Date", "Farm ID", "Error Type",
            "Error Message", "Logged At", "id"
        }

        self.assertSetEqual(actual_fields, expected_fields)

    def test_export_data(self):
        """Test exporting error logs."""
        dataset = self.resource.export(DCASErrorLog.objects.all())
        self.assertIsInstance(dataset, Dataset)
        self.assertEqual(len(dataset.dict), 1)

        exported_data = dataset.dict[0]
        self.assertEqual(
            exported_data["Request ID"], str(self.request.id)
        )
        self.assertEqual(
            exported_data["Farm ID"], self.farm.unique_id
        )
        self.assertEqual(
            exported_data["Error Type"], DCASErrorType.MISSING_MESSAGES
        )
        self.assertEqual(
            exported_data["Error Message"], "Test missing message error"
        )
