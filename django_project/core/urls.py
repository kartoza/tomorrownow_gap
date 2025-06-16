"""Tomorrow Now GAP."""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path

from core.views import (
    PreferencesRedirectView, FlowerProxyView
)
from frontend.api_views.auth.login import (
    LoginView as CustomLoginView, LogoutView as CustomLogoutView
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
        "auth/login/", CustomLoginView.as_view(),
        name="app_login"
    ),
    path(
        "auth/logout/", CustomLogoutView.as_view(),
        name="app_logout"
    ),
    # dj-rest-auth endpoints
    path(
        "auth/social/", include("allauth.socialaccount.urls")
    ),
    path("auth/registration/", include("dj_rest_auth.registration.urls")),
    path("auth/", include("dj_rest_auth.urls")),
    path("accounts/", include("allauth.urls")),
    path('admin/', admin.site.urls),
    path('', include('dcas.urls')),
    path('', include('frontend.urls')),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )
if settings.DEV_USE_BUNDLE_BUILD:
    urlpatterns += static(
        settings.STATIC_URL, document_root=settings.STATIC_ROOT
    )
