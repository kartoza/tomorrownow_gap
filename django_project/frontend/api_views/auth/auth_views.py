"""
Tomorrow Now GAP.

.. note:: authentication views.
"""
from knox.models import AuthToken
from knox.views import LogoutView, LogoutAllView
from allauth.account.utils import complete_signup
from allauth.account import app_settings as allauth_settings
from allauth.socialaccount.providers.google.views import GoogleOAuth2Adapter
from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter
from dj_rest_auth.views import LoginView
from dj_rest_auth.registration.views import RegisterView, SocialLoginView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from gap_api.serializers.user import UserInfoSerializer


# --- login ---
class KnoxLoginView(LoginView):
    """
    Returns Knox token in dj-rest-auth login format.

    Example response on successful login:
    { "key": "<token>", "user": {…} }
    """

    def get_response(self):
        """Override to return Knox token instead of default token."""
        # create token for self.user set in LoginView.login()
        _, token = AuthToken.objects.create(self.user)
        return Response(
            {"key": token, "user": UserInfoSerializer(self.user).data}
        )


# --- registration ---
class KnoxRegisterView(RegisterView):
    """On successful signup immediately log the user in & return token."""

    def get_success_url(self):
        """Override to redirect to the root URL after registration."""
        return "/"

    def get_response_data(self, user):
        """Just return success message, not a token."""
        return {
            "detail": (
                "Verification email sent. "
                "Please verify to activate your account."
            )
        }

    def perform_create(self, serializer):
        """Save the user and trigger email verification."""
        user = serializer.save(self.request)
        complete_signup(
            self.request, user,
            allauth_settings.EMAIL_VERIFICATION,
            self.get_success_url()
        )


# --- logout (single token & all tokens) ---
class KnoxLogoutView(LogoutView):
    """POST → revoke current token."""

    pass


class KnoxLogoutAllView(LogoutAllView):
    """POST → revoke all tokens for the user."""

    pass


# --- social login ---
ADAPTERS = {
    "google": GoogleOAuth2Adapter,
    "github": GitHubOAuth2Adapter,
    # "apple": AppleOAuth2Adapter,
}


class KnoxSocialLoginView(SocialLoginView):
    """Returns Knox token in dj-rest-auth social login format."""

    def get_serializer(self, *args, **kwargs):
        """Override to select the correct adapter based on the provider."""
        provider = (
            self.request.data.get("provider")
            or self.request.query_params.get("provider")
        )
        try:
            self.adapter_class = ADAPTERS[provider]
        except KeyError:
            raise ValidationError({"provider": "Unsupported provider"})
        return super().get_serializer(*args, **kwargs)


    def get_response(self):
        """Override to return Knox token instead of default token."""
        # self.user is set after successful social login
        _, token = AuthToken.objects.create(self.user)
        return Response(
            {"key": token, "user": self.get_serializer(self.user).data}
        )
