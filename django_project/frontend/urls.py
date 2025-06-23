"""Tomorrow Now GAP."""

from django.urls import path, re_path

from .views import (
    HomeView, SentryProxyView, EmailCheckView,
    OutputDownloadView, OutputListView,
    PermittedPagesView
)
from frontend.api_views import (
    UserFromUIDView, SignUpRequestView,
    SignUpRequestStatusView,
    MySignUpRequestView, ForgotPasswordView,
    ResetPasswordConfirmView, DecodeSocialSignupTokenView,
    APIKeyListCreate, APIKeyDestroy
)


urlpatterns = [
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    # /outputs/<pk>/download/
    path(
        "outputs/<int:pk>/download/",
        OutputDownloadView.as_view(),
        name="output-download",
    ),
    # list all recent outputs
    path(
        "outputs/",
        OutputListView.as_view(),
        name="output-list"
    ),
    path(
        "api/permitted-pages/",
        PermittedPagesView.as_view(),
        name="permitted-pages"
    ),
    path(
        'signup/check_email/', EmailCheckView.as_view(), name='check_email'
    ),
    path("api/signup-request/me/", MySignUpRequestView.as_view()),
    path(
        "api/signup-request-check/",
        SignUpRequestStatusView.as_view(),
        name="signup-request-check"
    ),
    path(
        "api/social-signup-token/",
        DecodeSocialSignupTokenView.as_view(),
        name="decode_social_signup_token"
    ),
    path(
        'api/signup-request/',
        SignUpRequestView.as_view(), name='signup-request'
    ),
    path(
        'password-reset/',
        ForgotPasswordView.as_view(), name='password-reset'
    ),
    path(
        'password-reset/confirm/<uidb64>/<token>/',
        ResetPasswordConfirmView.as_view(),
        name='password-reset-confirm'
    ),
    path(
        'api/user-uid/<str:uid>/',
        UserFromUIDView.as_view(), name='user-uid'
    ),
    # API Key management
    path(
        "api-keys/",
        APIKeyListCreate.as_view(),
        name="api_key_list_create"
    ),
    path(
        "api-keys/<str:key_id>/",
        APIKeyDestroy.as_view(),
        name="api_key_destroy"
    ),
    re_path(r'', HomeView.as_view(), name='home'),
]
