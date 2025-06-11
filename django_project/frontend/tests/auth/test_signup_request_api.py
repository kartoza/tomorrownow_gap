# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for SignUpRequest API.
"""

import mock
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from django.contrib.auth import get_user_model

from gap.models import SignUpRequest


class SignUpRequestAPITests(APITestCase):
    """Tests for the SignUpRequest API."""

    @mock.patch(
        'frontend.api_views.auth.signup_request.send_email_confirmation'
    )
    def test_signup_request_success(self, mock_send_email_confirmation):
        """Test successful sign-up request creation."""
        url = reverse('signup-request')
        data = {
            "first_name": "Test",
            "last_name": "User",
            "email": "testuser@example.com",
            "organization": "Test Organization",
            "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("email_verified", response.data)
        self.assertFalse(response.data["email_verified"])
        mock_send_email_confirmation.assert_called_once()

    def test_signup_request_missing_fields(self):
        """Test that missing fields return a 400 error."""
        url = reverse('signup-request')
        data = {
            "first_name": "Test",
            "last_name": "User",
            # "email": "testuser@example.com",
            # "organization": "Test Organization",
            # "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Email", response.data['detail'])

        data = {
            # "first_name": "Test",
            # "last_name": "User",
            "email": "testuser@example.com",
            # "organization": "Test Organization",
            # "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("First name", response.data['detail'])

        data = {
            "first_name": "Test",
            "last_name": "User",
            "email": "testuser@example.com",
            # "organization": "Test Organization",
            # "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Organization", response.data['detail'])

        data = {
            "first_name": "Test",
            "last_name": "User",
            "email": "testuser@example.com",
            "organization": "Test Organization",
            # "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Description", response.data['detail'])

    def test_signup_request_invalid_email(self):
        """Test with an invalid email format."""
        url = reverse('signup-request')
        data = {
            "first_name": "Test",
            "last_name": "User",
            "email": "not-an-email",
            "organization": "Test Organization",
            "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid email format", response.data['detail'])

    def test_signup_request_duplicate_email(self):
        """Test that a request cannot be created for an existing email."""
        User = get_user_model()
        User.objects.create_user(
            username="existinguser",
            email="existing@example.com",
            password="password"
        )
        # create a sign-up request for the existing email
        SignUpRequest.objects.create(
            first_name="Existing",
            last_name="User",
            email="existing@example.com",
            organization="Existing Organization",
            description="Requesting access to the platform.",
        )
        url = reverse('signup-request')
        data = {
            "first_name": "Test",
            "last_name": "User",
            "email": "existing@example.com",
            "organization": "Test Organization",
            "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertIn(
            response.status_code,
            [status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT]
        )
        self.assertIn(
            "A sign-up request for this email already exists.",
            response.data['detail']
        )

    def test_signup_request_existing_user(self):
        """Test that a request cannot be created for an existing user."""
        User = get_user_model()
        User.objects.create_user(
            username="existinguser",
            email="test@test.com",
            password="password",
            is_active=True
        )
        url = reverse('signup-request')
        data = {
            "first_name": "Test",
            "last_name": "User",
            "email": "test@test.com",
            "organization": "Test Organization",
            "description": "Requesting access to the platform.",
        }
        response = self.client.post(url, data, format='json')
        self.assertIn(
            response.status_code,
            [status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT]
        )
        self.assertIn(
            "User with this email already exists.",
            response.data['detail']
        )
