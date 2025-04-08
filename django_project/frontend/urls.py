"""Tomorrow Now GAP."""

from django.urls import path

from .views import (
    HomeView, SentryProxyView, SignupView,
    SignupRequestView, LoginView, EmailCheckView
)

urlpatterns = [
    path('', HomeView.as_view(), name='home'),
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    path('signup/', SignupView.as_view(), name='signup'),
    path(
        'signup/check_email/', EmailCheckView.as_view(), name='check_email'
    ),
    path(
        "signup-request/", SignupRequestView.as_view(), name="signup-request"
    ),
    path('login/', LoginView.as_view(), name='login'),
]
