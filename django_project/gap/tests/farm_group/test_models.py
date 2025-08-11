# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase

from gap.factories import (
    FarmGroupFactory, FarmFactory, UserF
)
from gap.models import FarmGroup, Preferences


class FarmGroupCRUDTest(TestCase):
    """Farm Group test case."""

    Factory = FarmGroupFactory
    Model = FarmGroup
    fixtures = [
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
        new_name = "New ID"
        obj.name = new_name
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.name, new_name)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmGroupFunctionalityCRUDTest(TestCase):
    """Farm Group test case."""

    Factory = FarmGroupFactory
    Model = FarmGroup
    fixtures = [
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
        group_1 = self.Factory()
        group_1.farms.add(FarmFactory(), FarmFactory(), FarmFactory())
        group_1.users.add(UserF(), UserF(), UserF())
        group_2 = self.Factory()
        group_2.farms.add(FarmFactory())
        group_2.users.add(UserF())

        self.group_1 = group_1
        self.group_2 = group_2

    def test_check_many_to_many(self):
        """Test read object."""
        self.assertEqual(self.group_1.farms.count(), 3)
        self.assertEqual(self.group_2.farms.count(), 1)
        self.assertEqual(self.group_1.users.count(), 3)
        self.assertEqual(self.group_2.users.count(), 1)

    def test_get_group_for_crop_insights(self):
        """Test get group for crop insights."""
        self.assertEqual(
            FarmGroup.get_group_for_crop_insights().count(),
            2
        )
        preferences = Preferences.load()
        preferences.crop_plan_config = {
            'farm_groups': [self.group_1.name]
        }
        preferences.save()
        self.assertEqual(
            FarmGroup.get_group_for_crop_insights().count(),
            1
        )
