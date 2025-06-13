"""frontend/tests/test_reset_password_api.py"""
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from django.utils.http import urlsafe_base64_encode
from django.core import signing

from rest_framework import status
from rest_framework.test import APITestCase

from unittest import mock

RP_MODULE = "frontend.api_views.auth.reset_password"


@override_settings(DEFAULT_FROM_EMAIL="noreply@example.com")
class ForgotPasswordAPITests(APITestCase):
    """Unit-tests for ForgotPasswordView."""

    def setUp(self) -> None:
        """Set up test user and URL for password reset."""
        self.User = get_user_model()
        self.url = reverse("password-reset")
        self.user = self.User.objects.create_user(
            username="test",
            email="test@example.com",
            password="pass",
            is_active=True,
        )

    @mock.patch(f"{RP_MODULE}.EmailMultiAlternatives")
    @mock.patch(f"{RP_MODULE}.render_to_string", return_value="stub")
    def test_forgot_password_success(
        self, mock_render, mock_email_cls
    ):
        """Valid email → 200 and email sent once."""
        payload = {"email": "test@example.com"}
        response = self.client.post(self.url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data["message"],
            "Password reset link sent to your email.",
        )
        mock_email_cls.assert_called_once()
        mock_email_cls.return_value.attach_alternative.assert_called_once()
        mock_email_cls.return_value.send.assert_called_once()

    def test_forgot_password_email_not_found(self):
        """Non-existent email → 400 with error message."""
        payload = {"email": "missing@example.com"}
        response = self.client.post(self.url, payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"], "Email not found")

    def test_forgot_password_multiple_users(self):
        """Multiple users with same email, 400 with error message."""
        self.User.objects.create_user(
            username="alt",
            email="test@example.com",
            password="pass",
            is_active=False,
        )
        payload = {"email": "test@example.com"}
        response = self.client.post(self.url, payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["error"],
            "Multiple users with this email address"
        )


class ResetPasswordConfirmAPITests(APITestCase):
    """Unit-tests for ResetPasswordConfirmView."""

    def setUp(self) -> None:
        """Set up test user and URL for password reset confirmation."""
        self.User = get_user_model()
        self.user = self.User.objects.create_user(
            username="test",
            email="test@example.com",
            password="oldpass",
            is_active=True,
        )
        self.token = signing.dumps(self.user.pk)
        self.uidb64 = urlsafe_base64_encode(str(self.user.pk).encode())

        self.url = reverse(
            "password-reset-confirm",
            kwargs={"uidb64": self.uidb64, "token": self.token},
        )

    @mock.patch(
            f"{RP_MODULE}.default_token_generator.check_token",
            return_value=True
    )
    def test_reset_password_success(self, mock_check):
        """Valid token and uid → 200 and password updated."""
        payload = {"new_password": "newpass123"}
        response = self.client.post(self.url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("newpass123"))

    @mock.patch(
            f"{RP_MODULE}.default_token_generator.check_token",
            return_value=False
    )
    def test_reset_password_invalid_token(self, mock_check):
        """Invalid token → 400 with error message."""
        payload = {"new_password": "whatever"}
        response = self.client.post(self.url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"], "Invalid reset link")

    def test_reset_password_invalid_uid(self):
        """Invalid uidb64 → 400 with error message."""
        bad_uid_url = reverse(
            "password-reset-confirm",
            kwargs={"uidb64": "!!!", "token": "irrelevant"},
        )
        response = self.client.post(bad_uid_url, {}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["error"], "Invalid reset link")
