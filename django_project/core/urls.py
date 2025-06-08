"""Tomorrow Now GAP."""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path

from core.views import (
    PreferencesRedirectView, FlowerProxyView
)
from frontend.api_views.auth import (
    KnoxRegisterView,
    KnoxLogoutView, KnoxLogoutAllView,
    KnoxSocialLoginView
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
    # Knox token endpoints
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
    path(
        "auth/social/", include("allauth.socialaccount.urls")
    ),
    path("auth/", include("dj_rest_auth.registration.urls")),
    path("auth/", include("dj_rest_auth.urls")),
    path("accounts/", include("allauth.urls")),
    path('admin/', admin.site.urls),
    path('', include('frontend.urls')),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )
