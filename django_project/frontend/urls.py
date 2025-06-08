"""Tomorrow Now GAP."""

from django.urls import path, re_path

from .views import (
    HomeView, SentryProxyView, EmailCheckView
)
from frontend.api_views import (
    UserFromUIDView, SignUpRequestView,
    SignUpRequestStatusView, CurrentUserView,
    MySignUpRequestView,
    KnoxLoginView, KnoxRegisterView,
    KnoxLogoutAllView, KnoxLogoutView,
    KnoxSocialLoginView
)


urlpatterns = [
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    path(
        'signup/check_email/', EmailCheckView.as_view(), name='check_email'
    ),
    # Knox token endpoints
    path(
        "auth/login/", KnoxLoginView.as_view(),
        name="knox_login"
    ),
    path(
        "auth/registration/", KnoxRegisterView.as_view(),
        name="knox_signup"
    ),
    path(
        "auth/logout/", KnoxLogoutView.as_view(),
        name="knox_logout"
    ),
    path(
        "auth/logoutall/", KnoxLogoutAllView.as_view(),
        name="knox_logoutall"
    ),
    # dj-rest-auth endpoints
    path(
        "auth/social/login/", KnoxSocialLoginView.as_view(),
        name='social_login'
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
        'api/user-uid/<str:uid>/',
        UserFromUIDView.as_view(), name='user-uid'
    ),
    re_path(r'', HomeView.as_view(), name='home'),
]
