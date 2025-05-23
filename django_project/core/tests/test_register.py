# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for register view.
"""

from django.test import TestCase
from django.urls import reverse
from django.contrib.auth import get_user_model
from django.utils.http import urlsafe_base64_encode
from django.utils.encoding import force_bytes
from django.contrib.auth.tokens import default_token_generator
from rest_framework.test import APIClient
from rest_framework import status

User = get_user_model()


class VerifyEmailViewTests(TestCase):
    """Test the verify email view."""

    def setUp(self):
        """Set up the test."""
        self.client = APIClient()
        self.user = User.objects.create(
            first_name="Test",
            last_name="Smith",
            email="Test@example.com",
            is_active=False,
        )
        self.user.set_password("securepassword")
        self.user.save()

        self.uid = urlsafe_base64_encode(force_bytes(self.user.pk))
        self.token = default_token_generator.make_token(self.user)
        self.url = reverse("verify-email")

    def test_verify_email_success(self):
        """Test verifying email."""
        response = self.client.get(
            self.url, {"uid": self.uid, "token": self.token}
        )
        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertEqual(
            response.url,
            f"/signup-request/?uid={self.uid}&token={self.token}")
        self.user.refresh_from_db()

    def test_verify_email_invalid_token(self):
        """Test verifying email with invalid token."""
        response = self.client.get(
            self.url, {"uid": self.uid, "token": "invalid-token"}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid or expired token", response.data["detail"])

    def test_verify_email_missing_parameters(self):
        """Test verifying email with missing parameters."""
        response = self.client.get(self.url, {})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Missing parameters", response.data["detail"])

    def test_verify_email_invalid_uid(self):
        """Test verifying email with invalid uid."""
        response = self.client.get(
            self.url, {"uid": "invalid", "token": self.token}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid token or user", response.data["detail"])
