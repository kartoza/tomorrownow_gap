# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Prise Message Models.
"""

from django.test import TestCase

from message.factories import MessageTemplateFactory
from prise.factories import PriseMessageFactory, PestFactory
from prise.models import PriseMessage


class PriseMessageCRUDTest(TestCase):
    """PriseMessage test case."""

    Factory = PriseMessageFactory
    Model = PriseMessage

    def test_create_object(self):
        """Test create object."""
        obj = self.Factory()
        obj.messages.add(
            *[MessageTemplateFactory(), MessageTemplateFactory()]
        )
        self.assertIsInstance(obj, self.Model)
        self.assertTrue(self.Model.objects.filter(id=obj.id).exists())
        self.assertTrue(
            self.Model.objects.filter(id=obj.id).first().messages.count(), 2
        )

    def test_read_object(self):
        """Test read object."""
        obj = self.Factory()
        fetched_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_object(self):
        """Test update object."""
        obj = self.Factory()
        pest = PestFactory()
        obj.pest = pest
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.pest, pest)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())
