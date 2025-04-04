"""Tomorrow Now GAP."""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path

from core.views import (
    PreferencesRedirectView, FlowerProxyView,
    RegisterView, VerifyEmailView, SignUpRequestView
)


urlpatterns = [
    re_path(
        r'^api/', include(('gap_api.urls', 'api'), namespace='api')
    ),
    re_path(
        r'^admin/gap/preferences/$', PreferencesRedirectView.as_view(),
        name='index'
    ),
    FlowerProxyView.as_url(),
    path(
        'api/signup-request/',
        SignUpRequestView.as_view(),
        name='signup-request'
    ),
    path(
        'api/auth/register/',
        RegisterView.as_view(),
        name='register'
    ),
    path(
        'api/auth/verify-email/',
        VerifyEmailView.as_view(), name='verify-email'
    ),
    path('admin/', admin.site.urls),
    path('', include('frontend.urls')),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )
