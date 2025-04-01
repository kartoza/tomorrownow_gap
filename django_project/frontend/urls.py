"""Tomorrow Now GAP."""

from django.urls import path

from .views import HomeView, SentryProxyView, SignupView

urlpatterns = [
    path('', HomeView.as_view(), name='home'),
    path('sentry-proxy/', SentryProxyView.as_view(), name='sentry-proxy'),
    path('signup/', SignupView.as_view(), name='signup'),
]
