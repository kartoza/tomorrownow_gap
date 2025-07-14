# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GAP API v1 urls.
"""
import json
from django.db.utils import ProgrammingError
from django.urls import include, re_path, path
from drf_yasg import openapi
from drf_yasg.renderers import SwaggerUIRenderer, ReDocRenderer
from drf_yasg.views import get_schema_view, UI_RENDERERS
from rest_framework import permissions, authentication

from gap.models.preferences import Preferences
from gap_api.api_views.crop_insight import CropPlanAPI
from gap_api.api_views.measurement import (
    MeasurementAPI,
    product_type_list,
    dataset_attribute_by_product,
    JobStatusAPI,
    MeasurementOptionsView
)
from gap_api.api_views.user import UserInfo
from gap_api.api_views.location import LocationAPI
from gap_api.urls.schema import CustomSchemaGenerator


class TomorrowNowSwaggerUIRenderer(SwaggerUIRenderer):
    """The Swagger renderer that specifically for Tomorrow Now GAP."""

    template = 'gap/swagger-ui.html'

    def set_context(self, renderer_context, swagger=None):
        """Add custom context to the renderer."""
        super().set_context(renderer_context, swagger)

        # Add product type list
        renderer_context['product_type_list'] = json.dumps(product_type_list())
        # Add dataset attribute list
        renderer_context['attribute_dict'] = (
            json.dumps(dataset_attribute_by_product())
        )


UI_RENDERERS['tomorrownow'] = (TomorrowNowSwaggerUIRenderer, ReDocRenderer)

try:
    preferences = Preferences.load()
except ProgrammingError:
    preferences = Preferences()
except RuntimeError:
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
        'measurement/job-status/<str:job_id>/',
        JobStatusAPI.as_view(),
        name='measurement-job-status'
    ),
    path(
        'measurement/',
        MeasurementAPI.as_view(),
        name='get-measurement'
    ),
    path(
        'measurement/options/',
        MeasurementOptionsView.as_view(),
        name='measurement-options'
    ),
]

# LOCATION API
location_urls = [
    path(
        'location/',
        LocationAPI.as_view(),
        name='upload-location'
    ),
]

urlpatterns = [
    re_path(
        r'^docs/$',
        schema_view_v1.with_ui('tomorrownow', cache_timeout=0),
        name='schema-swagger'
    ),
]
urlpatterns += user_urls
urlpatterns += location_urls
urlpatterns += measurement_urls
urlpatterns += [
    re_path(
        r'crop-plan/$',
        CropPlanAPI.as_view(),
        name='crop-plan'
    )
]
