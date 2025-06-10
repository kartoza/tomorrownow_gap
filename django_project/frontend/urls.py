"""Tomorrow Now GAP."""

from django.urls import path, re_path

from .views import (
    HomeView, SentryProxyView, EmailCheckView
)
from frontend.api_views import (
    UserFromUIDView, SignUpRequestView,
    SignUpRequestStatusView, CurrentUserView,
    MySignUpRequestView, ForgotPasswordView,
    ResetPasswordConfirmView
)


urlpatterns = [
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    path(
        'signup/check_email/', EmailCheckView.as_view(), name='check_email'
    ),
    path("api/signup-request/me/", MySignUpRequestView.as_view()),
    path("api/me/", CurrentUserView.as_view(), name="current-user"),
    path(
        "api/signup-request-check/",
        SignUpRequestStatusView.as_view(),
        name="signup-request-check"
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
    re_path(r'', HomeView.as_view(), name='home'),
]
