"""frontend/tests/test_social_signup_adapter.py."""
from types import SimpleNamespace
from urllib.parse import urlparse, parse_qs

import django.core.signing as signing
from django.test import TestCase, RequestFactory
from unittest import mock
from django.contrib.auth import get_user_model

from gap.models import SignUpRequest, RequestStatus
from frontend.adapters import (
    SocialSignupAdapter,
    InactiveRedirectAccountAdapter,
)


SALT = "social-signup"


class SocialSignupAdapterTests(TestCase):
    """Unit-tests for SocialSignupAdapter."""

    def setUp(self):
        """Set up test user and request factory."""
        self.factory = RequestFactory()
        self.User = get_user_model()

        self.user = self.User.objects.create_user(
            username="newbie",
            email="newbie@example.com",
            password="pass",
            first_name="New",
            last_name="Bie",
            is_active=True,
        )

    @mock.patch(
        "allauth.socialaccount.adapter.DefaultSocialAccountAdapter.save_user",
        autospec=True,
    )
    def test_save_user_inactivates_and_upserts_request(self, mock_super_save):
        """
        After a social login/signup.

        • user.is_active -> False
        • SignUpRequest upserted with PENDING
        """
        request = self.factory.post("/dummy")
        dummy_sociallogin = SimpleNamespace(is_existing=True)

        mock_super_save.return_value = self.user

        adapter = SocialSignupAdapter()
        adapter.save_user(request, dummy_sociallogin, form=None)

        self.user.refresh_from_db()
        self.assertFalse(self.user.is_active)

        req = SignUpRequest.objects.get(email=self.user.email)
        self.assertEqual(req.status, RequestStatus.PENDING)
        self.assertEqual(req.first_name, "New")
        self.assertEqual(req.last_name, "Bie")

    def _assert_redirect_has_valid_token(self, response):
        """Assert response is a redirect to /signup with a valid token."""
        self.assertEqual(response.status_code, 302)
        loc = response["Location"]
        parsed = urlparse(loc)
        self.assertEqual(parsed.path, "/signup")

        token = parse_qs(parsed.query)["token"][0]
        data = signing.loads(token, salt=SALT)

        self.assertEqual(
            data,
            {
                "email": "newbie@example.com",
                "first_name": "New",
                "last_name": "Bie",
            },
        )

    def test_social_adapter_respond_user_inactive(self):
        """Test the social adapter's response for inactive user."""
        adapter = SocialSignupAdapter()
        request = self.factory.get("/dummy")
        response = adapter.respond_user_inactive(request, self.user)
        self._assert_redirect_has_valid_token(response)

    def test_account_adapter_respond_user_inactive(self):
        """Test the account adapter's response for inactive user."""
        adapter = InactiveRedirectAccountAdapter()
        request = self.factory.get("/dummy")
        response = adapter.respond_user_inactive(request, self.user)
        self._assert_redirect_has_valid_token(response)
