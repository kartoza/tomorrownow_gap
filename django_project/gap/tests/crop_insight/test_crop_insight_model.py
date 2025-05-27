# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

import uuid
from datetime import datetime, timezone

from django.test import TestCase

from gap.factories import CropInsightRequestFactory, FarmGroupFactory
from gap.models import CropInsightRequest


class CropCRUDTest(TestCase):
    """Crop test case."""

    Factory = CropInsightRequestFactory
    Model = CropInsightRequest
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

    def test_create_object(self):
        """Test create object."""
        obj = self.Factory()
        self.assertIsInstance(obj, self.Model)
        self.assertTrue(self.Model.objects.filter(id=obj.id).exists())

    def test_read_object(self):
        """Test read object."""
        obj = self.Factory()
        fetched_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_object(self):
        """Test update object."""
        obj = self.Factory()
        new_uuid = uuid.uuid4()
        obj.unique_id = new_uuid
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.unique_id, new_uuid)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())

    def test_title(self):
        """Test update object."""
        group = FarmGroupFactory()
        obj = self.Factory(
            farm_group=group
        )
        obj.requested_at = datetime(
            2024, 1, 1, 0, 0, 0,
            tzinfo=timezone.utc
        )
        obj.save()
        self.assertEqual(
            obj.title, (
                f"GAP - Crop Plan Generator Results - {group.name} - "
                "Monday-01-01-2024 (UTC+03:00)"
            )
        )

        # Change to before 23, so east time should tomorrow
        obj.requested_at = datetime(
            2024, 1, 1, 23, 0, 0,
            tzinfo=timezone.utc
        )
        obj.save()
        self.assertEqual(
            obj.title, (
                f"GAP - Crop Plan Generator Results - {group.name} - "
                "Tuesday-02-01-2024 (UTC+03:00)"
            )
        )

        # Change to before 22, so east time should tomorrow
        obj.requested_at = datetime(
            2024, 1, 1, 22, 0, 0,
            tzinfo=timezone.utc
        )
        obj.save()
        self.assertEqual(
            obj.title, (
                f"GAP - Crop Plan Generator Results - {group.name} - "
                "Tuesday-02-01-2024 (UTC+03:00)"
            )
        )

        # Change to before 21, so east time should today
        obj.requested_at = datetime(
            2024, 1, 1, 20, 0, 0,
            tzinfo=timezone.utc
        )
        obj.save()
        self.assertEqual(
            obj.title, (
                f"GAP - Crop Plan Generator Results - {group.name} - "
                "Monday-01-01-2024 (UTC+03:00)"
            )
        )
