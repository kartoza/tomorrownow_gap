"""Tomorrow Now GAP."""

from django.urls import path, re_path

from .views import (
    HomeView, SentryProxyView, SignupView,
    SignupRequestView, LoginView, EmailCheckView
)

urlpatterns = [
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    path('signup/', SignupView.as_view(), name='signup'),
    path(
        'signup/check_email/', EmailCheckView.as_view(), name='check_email'
    ),
    path(
        "signup-request/", SignupRequestView.as_view(), name="signup-request"
    ),
    path('login/', LoginView.as_view(), name='login'),
    re_path(r'', HomeView.as_view(), name='home'),
]
