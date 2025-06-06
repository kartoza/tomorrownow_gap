"""Tomorrow Now GAP."""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path

from core.views import (
    PreferencesRedirectView, FlowerProxyView,
    UserFromUIDView, SignUpRequestView,
    SignUpRequestStatusView, CurrentUserView,
    MySignUpRequestView,
    KnoxLoginView, KnoxRegisterView,
    KnoxLogoutAllView, KnoxLogoutView,
    KnoxSocialLoginView
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
    path(
        "auth/social/", include("allauth.socialaccount.urls")
    ),
    path("auth/", include("dj_rest_auth.registration.urls")),
    path("auth/", include("dj_rest_auth.urls")),
    path("accounts/", include("allauth.urls")),
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
    path('admin/', admin.site.urls),
    path('', include('frontend.urls')),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )
