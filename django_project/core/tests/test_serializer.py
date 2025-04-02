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
    """Test the RegisterSerializer."""

    def test_valid_data_creates_user(self):
        """Test that valid data creates a user."""
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
        """Test that password mismatch raises an error."""
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
        """Test that missing required fields raises an error."""
        serializer = RegisterSerializer(data={})
        self.assertFalse(serializer.is_valid())
        self.assertIn("first_name", serializer.errors)
        self.assertIn("last_name", serializer.errors)
        self.assertIn("email", serializer.errors)
