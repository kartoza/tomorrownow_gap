# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for SignUpRequest model.
"""

from django.test import TestCase
from django.db.utils import IntegrityError

from gap.models import SignUpRequest
from gap.factories import SignUpRequestFactory


class SignUpRequestCRUDTest(TestCase):
    """SignUpRequest test case."""

    def test_create(self):
        """Test create object."""
        obj = SignUpRequestFactory()
        self.assertIsInstance(obj, SignUpRequest)
        self.assertTrue(SignUpRequest.objects.filter(id=obj.id).exists())

        # Creating another with same email (unique) should fail
        with self.assertRaises(IntegrityError):
            SignUpRequestFactory(email=obj.email)

    def test_read(self):
        """Test read object."""
        obj = SignUpRequestFactory()
        fetched = SignUpRequest.objects.get(id=obj.id)
        self.assertEqual(fetched, obj)

    def test_update(self):
        """Test update object."""
        obj = SignUpRequestFactory()
        obj.first_name = "Updated"
        obj.save()
        updated = SignUpRequest.objects.get(id=obj.id)
        self.assertEqual(updated.first_name, "Updated")

    def test_delete(self):
        """Test delete object."""
        obj = SignUpRequestFactory()
        obj_id = obj.id
        obj.delete()
        self.assertFalse(SignUpRequest.objects.filter(id=obj_id).exists())
