# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GAP API v1 urls.
"""
from django.db.utils import ProgrammingError
from django.urls import include, re_path, path
from drf_yasg import openapi
from drf_yasg.renderers import SwaggerUIRenderer, ReDocRenderer
from drf_yasg.views import get_schema_view, UI_RENDERERS
from rest_framework import permissions, authentication

from gap.models.preferences import Preferences
from gap_api.api_views.crop_insight import CropPlanAPI
from gap_api.api_views.measurement import MeasurementAPI
from gap_api.api_views.user import UserInfo
from gap_api.urls.schema import CustomSchemaGenerator


class TomorrowNowSwaggerUIRenderer(SwaggerUIRenderer):
    """The Swagger renderer that specifically for Tomorrow Now GAP."""

    template = 'gap/swagger-ui.html'


UI_RENDERERS['tomorrownow'] = (TomorrowNowSwaggerUIRenderer, ReDocRenderer)

try:
    preferences = Preferences.load()
except ProgrammingError:
    preferences = Preferences()

schema_view_v1 = get_schema_view(
    openapi.Info(
        title="Global Access Platform API",
        description=(
            f'''
            <a href="{preferences.documentation_url}" target="_blank">
                Read API Documentation
            </a>
            '''
        ),
        default_version='v0.0.1'
    ),
    public=True,
    authentication_classes=[authentication.SessionAuthentication],
    permission_classes=[permissions.AllowAny],
    generator_class=CustomSchemaGenerator,
    patterns=[
        re_path(
            r'^api/',
            include((
                [
                    re_path(
                        r'^v1/',
                        include(('gap_api.urls.v1', 'v1'), namespace='v1')
                    )
                ], 'api'),
                namespace='api'
            )
        )
    ],
)

# USER API
user_urls = [
    path(
        'user/me',
        UserInfo.as_view(),
        name='user-info'
    ),
]

# MEASUREMENT APIs
measurement_urls = [
    path(
        'measurement/',
        MeasurementAPI.as_view(),
        name='get-measurement'
    )
]
urlpatterns = [
    re_path(
        r'^docs/$',
        schema_view_v1.with_ui('tomorrownow', cache_timeout=0),
        name='schema-redoc'
    ),
]
urlpatterns += user_urls
urlpatterns += measurement_urls
urlpatterns += [
    re_path(
        r'crop-plan/$',
        CropPlanAPI.as_view(),
        name='crop-plan'
    )
]
