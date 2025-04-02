# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for SignUpRequest model.
"""

from django.test import TestCase
from django.db.utils import IntegrityError

from gap.models import SignUpRequest
from gap.factories import SignUpRequestFactory
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group


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


User = get_user_model()


class TestSignUpRequestSignal(TestCase):
    """Test email is sent to managers on sign-up request."""

    def setUp(self):
        """Create User Manager group."""
        self.manager_group = Group.objects.create(name="User Manager")
        self.manager = User.objects.create_user(
            username="manager1",
            email="manager@example.com",
            password="password",
            is_active=True,
            is_staff=True,
        )
        self.manager.groups.add(self.manager_group)

    @patch("gap.models.signup_request.send_mail")
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
