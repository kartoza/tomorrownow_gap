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

    # TODO: To be updated. Tests were failing.
