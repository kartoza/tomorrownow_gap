# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tests for signals.
"""

from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from gap.models import SignUpRequest


User = get_user_model()


class TestSignUpRequestSignal(TestCase):
    """Test email is sent to managers on sign-up request."""

    def setUp(self):
        # Create User Manager group
        self.manager_group = Group.objects.create(name="User Manager")
        self.manager = User.objects.create_user(
            username="manager1",
            email="manager@example.com",
            password="password",
            is_active=True,
            is_staff=True,
        )
        self.manager.groups.add(self.manager_group)

    @patch("gap.signals.send_mail")
    def test_email_sent_on_signup_request(self, mock_send_mail):
        """Test email is sent to managers on sign-up request."""
        SignUpRequest.objects.create(
            first_name="Test",
            last_name="User",
            email="testuser@example.com",
            description="Requesting access.",
        )

        mock_send_mail.assert_called_once()
        args, kwargs = mock_send_mail.call_args

        assert "New Sign Up Request" in kwargs["subject"]
        assert "testuser@example.com" in kwargs["message"]
        assert self.manager.email in kwargs["recipient_list"]
