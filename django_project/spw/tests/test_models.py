# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from datetime import datetime
import pytz
from django.test import TestCase

from spw.factories import (
    SPWOutputFactory
)
from spw.models import (
    SPWOutput
)
from spw.generator.input import SPWDataInput


class SPWOutputCRUDTest(TestCase):
    """SPWOutput test case."""

    def test_create_obj(self):
        """Test create object."""
        obj = SPWOutputFactory()
        self.assertIsInstance(obj, SPWOutput)
        self.assertTrue(SPWOutput.objects.filter(id=obj.id).exists())

    def test_read_obj(self):
        """Test read object."""
        obj = SPWOutputFactory()
        fetched_obj = SPWOutput.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_obj(self):
        """Test update object."""
        obj = SPWOutputFactory()
        new_tier = "Tier 2"
        obj.tier = new_tier
        obj.save()
        updated_obj = SPWOutput.objects.get(id=obj.id)
        self.assertEqual(updated_obj.tier, new_tier)

    def test_delete_obj(self):
        """Test delete object."""
        obj = SPWOutputFactory()
        obj_id = obj.id
        obj.delete()
        self.assertFalse(SPWOutput.objects.filter(id=obj_id).exists())


class SPWDataInputTest(TestCase):
    """SPWDataInput test case."""

    def test_validate_valid_data(self):
        """Test validate method with valid data."""
        valid_data = {
            "10-01": {
                "date": "2023-10-01",
                "evapotranspirationSum": 0.5,
                "rainAccumulationSum": 1.0,
                "temperatureMax": 30.0,
                "temperatureMin": 15.0,
                "precipitationProbability": 0.8,
            }
        }
        data_input = SPWDataInput(
            0, 0,
            datetime(2023, 10, 1, 0, 0, tzinfo=pytz.UTC)  # current_date
        )
        data_input.data = valid_data
        self.assertTrue(data_input.validate())

    def test_validate_invalid_data(self):
        """Test validate method with invalid data."""
        invalid_data = {
            "10-01": {
                "date": "2023-10-01",
                "evapotranspirationSum": 0.5,
                "rainAccumulationSum": 1.0,
                "temperatureMax": 30.0,
                "temperatureMin": 15.0,
                "precipitationProbability": 0.8,
                "Test": 0.5,  # Extra field
            }
        }
        data_input = SPWDataInput(
            0, 0,
            datetime(2023, 10, 1, 0, 0, tzinfo=pytz.UTC)  # current_date
        )
        data_input.data = invalid_data
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("Invalid variable", str(context.exception))

        data_input.data = []
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("Data must be a dictionary", str(context.exception))

        data_input.data = {
            10: {
                "date": "2023-10-01",
                "evapotranspirationSum": 0.5,
                "rainAccumulationSum": 1.0,
                "temperatureMax": 30.0,
                "temperatureMin": 15.0,
                "precipitationProbability": 0.8,
            }
        }
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("month_day must be a string", str(context.exception))

        data_input.data = {
            "2001-1": {
                "date": "2023-10-01",
                "evapotranspirationSum": 0.5,
                "rainAccumulationSum": 1.0,
                "temperatureMax": 30.0,
                "temperatureMin": 15.0,
                "precipitationProbability": 0.8,
            }
        }
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("month_day must be in", str(context.exception))

        data_input.data = {
            "01-01": {
                "date": "2023-10-01",
                "evapotranspirationSum": 0.5,
                "rainAccumulationSum": 1.0,
                "temperatureMax": 30.0,
                "temperatureMin": 15.0,
                "precipitationProbability": 0.8,
            }
        }
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("is out of range", str(context.exception))

        data_input.data = {
            "10-01": []
        }
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("Values must be a dictionary", str(context.exception))

        data_input.data = {
            "10-01": {
                "date": "2023-10-01",
                "evapotranspirationSum": 0.5,
                "rainAccumulationSum": 1.0,
                "temperatureMax": 30.0,
                "temperatureMin": 15.0,
                "precipitationProbability": "test"
            }
        }
        with self.assertRaises(ValueError) as context:
            data_input.validate()
        self.assertIn("must be numeric", str(context.exception))
