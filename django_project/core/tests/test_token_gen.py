# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for token generation.
"""

from django.test import TestCase
from django.contrib.auth import get_user_model
from django.utils.http import urlsafe_base64_decode
from django.contrib.auth.tokens import default_token_generator

from core.utils.token_gen import generate_verification_token

User = get_user_model()


class TokenGeneratorTests(TestCase):
    """Test the token generation utility."""

    def setUp(self):
        """Set up the test."""
        self.user = User.objects.create_user(
            first_name="Alice",
            last_name="Smith",
            username="alice123",
            email="alice@example.com",
            password="securepass123",
            is_active=False,
        )

    def test_generate_verification_token(self):
        """Test generating a verification token."""
        uid, token = generate_verification_token(self.user)

        # UID should decode back to the user's ID
        decoded_id = urlsafe_base64_decode(uid).decode()
        self.assertEqual(str(self.user.pk), decoded_id)

        # Token should be valid for this user
        self.assertTrue(default_token_generator.check_token(self.user, token))
