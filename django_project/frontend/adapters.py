"""frontend/adapters.py."""
import logging
from django.shortcuts import redirect
from urllib.parse import urlencode
from allauth.account.adapter import DefaultAccountAdapter
from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from django.contrib.auth import get_user_model
import django.core.signing as signing
from gap.models import SignUpRequest, RequestStatus

User = get_user_model()

logger = logging.getLogger(__name__)


class SocialSignupAdapter(DefaultSocialAccountAdapter):
    """
    After a successful social login/signup.

    - mark the user inactive
    - create or update a SignUpRequest in INCOMPLETE state
    """

    def save_user(self, request, sociallogin, form=None):
        """Save the user after social login/signup."""
        user = super().save_user(request, sociallogin, form)

        # Always make the user inactive
        user.is_active = False
        user.save(update_fields=["is_active"])

        # Always create or update the signup request
        SignUpRequest.objects.update_or_create(
            email=user.email,
            defaults={
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "status": RequestStatus.INCOMPLETE,
            },
        )

        return user

    def respond_user_inactive(self, request, user):
        """Handle inactive user after social login/signup."""
        data = {
            "email": user.email or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
        }
        token = signing.dumps(data, salt="social-signup")
        logger.debug("Generated signup token for %s: %s", user, token)

        # send user straight to the frontend page with the token
        return redirect(f"/signup?{urlencode({'token': token})}")


class InactiveRedirectAccountAdapter(DefaultAccountAdapter):
    """Override ONLY the inactive-user response."""

    def respond_user_inactive(self, request, user):
        """Handle inactive user after login/signup."""
        data = {
            "email": user.email or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
        }
        token = signing.dumps(data, salt="social-signup")
        return redirect(f"/signup?{urlencode({'token': token})}")
