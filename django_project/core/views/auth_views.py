from knox.models import AuthToken
from knox.views import LogoutView as KnoxLogoutView, LogoutAllView
from allauth.socialaccount.providers.google.views import GoogleOAuth2Adapter
from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter
from dj_rest_auth.views import LoginView
from dj_rest_auth.registration.views import RegisterView, SocialLoginView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError


# --- login ---
class KnoxLoginView(LoginView):
    """
    Returns Knox token in dj-rest-auth login format:
    { "key": "<token>", "user": {…} }
    """

    def get_response(self):
        # create token for self.user set in LoginView.login()
        _, token = AuthToken.objects.create(self.user)
        return Response(
            {"key": token, "user": self.get_serializer(self.user).data}
        )


# --- registration ---
class KnoxRegisterView(RegisterView):
    """
    On successful signup immediately log the user in & return token.
    """

    def get_response_data(self, user):
        _, token = AuthToken.objects.create(user)
        return {"key": token, "user": self.get_serializer(user).data}


# --- logout (single token & all tokens) ---
class KnoxLogoutView(KnoxLogoutView):
    """POST → revoke current token"""

    pass


class KnoxLogoutAllView(LogoutAllView):
    """POST → revoke all tokens for the user"""

    pass


# --- social login ---
ADAPTERS = {
    "google": GoogleOAuth2Adapter,
    "github": GitHubOAuth2Adapter,
    # "apple": AppleOAuth2Adapter,
}


class KnoxSocialLoginView(SocialLoginView):
    def get_serializer(self, *args, **kwargs):
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
        # self.user is set after successful social login
        _, token = AuthToken.objects.create(self.user)
        return Response(
            {"key": token, "user": self.get_serializer(self.user).data}
        )
