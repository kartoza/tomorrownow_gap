"""frontend/tests/auth/test_auth_views.py."""
from django.core import signing
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


class DecodeSocialSignupTokenTests(APITestCase):
    """Unit-tests for /api/social-signup-token/ decoder endpoint."""

    endpoint = "decode_social_signup_token"
    salt = "social-signup"

    def test_missing_token_returns_204_empty(self):
        """Test that missing token returns 204 with empty response."""
        url = reverse(self.endpoint)
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 204)
        self.assertEqual(resp.data, {})

    def test_valid_token_returns_payload(self):
        """Test that valid token returns the original payload."""
        payload = {
            "email": "jane@example.com",
            "first_name": "Jane",
            "last_name": "Doe",
        }
        token = signing.dumps(payload, salt=self.salt)

        url = reverse(self.endpoint) + f"?token={token}"
        resp = self.client.get(url)

        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data, payload)

    def test_invalid_token_returns_400(self):
        """Test that invalid token returns 400 with error message."""
        bad_token = "this-is-not-valid"

        url = reverse(self.endpoint) + f"?token={bad_token}"
        resp = self.client.get(url)

        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Invalid token")
