# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for serializers.
"""

from django.test import TestCase
from django.contrib.auth import get_user_model
from core.serializers import RegisterSerializer

User = get_user_model()


class RegisterSerializerTests(TestCase):
    def test_valid_data_creates_user(self):
        data = {
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "password": "strongpassword",
            "confirm_password": "strongpassword",
        }
        serializer = RegisterSerializer(data=data)
        self.assertTrue(serializer.is_valid(), serializer.errors)
        user = serializer.save()
        self.assertEqual(user.email, "john@example.com")
        self.assertFalse(user.is_active)  # Since we use email verification

    def test_password_mismatch(self):
        data = {
            "first_name": "Jane",
            "last_name": "Doe",
            "email": "jane@example.com",
            "password": "pass123",
            "confirm_password": "pass456",
        }
        serializer = RegisterSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn("non_field_errors", serializer.errors)

    def test_missing_required_fields(self):
        serializer = RegisterSerializer(data={})
        self.assertFalse(serializer.is_valid())
        self.assertIn("first_name", serializer.errors)
        self.assertIn("last_name", serializer.errors)
        self.assertIn("email", serializer.errors)
