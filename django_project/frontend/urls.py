"""Tomorrow Now GAP."""

from django.urls import path

from .views import (
    HomeView, SentryProxyView, SignupView,
    SignupRequestView,
)

urlpatterns = [
    path('', HomeView.as_view(), name='home'),
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    path('signup/', SignupView.as_view(), name='signup'),
    path(
        "signup-request/", SignupRequestView.as_view(), name="signup-request"
    ),
]
