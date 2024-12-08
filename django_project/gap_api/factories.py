# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from factory.django import DjangoModelFactory
from django.contrib.gis.geos import Polygon, MultiPolygon

from core.factories import UserF
from gap_api.models import (
    APIRequestLog, Location, APIRateLimiter,
    UserFile
)


class APIRequestLogFactory(DjangoModelFactory):
    """Factory class for APIRequestLog model."""

    class Meta:  # noqa
        model = APIRequestLog

    user = factory.SubFactory(UserF)
    username_persistent = factory.Faker('name')
    path = '/api/v1/measurement'
    host = 'http://localhost'
    method = 'GET'
    query_params = {
        'lat': '-1.404244',
        'lon': '35.008688',
        'product': 'tahmo_ground_observation',
        'end_date': '2019-11-10',
        'attributes': [
            'max_relative_humidity',
            'min_relative_humidity'
        ],
        'start_date': '2019-11-01',
        'output_type': 'csv'
    }


class LocationFactory(DjangoModelFactory):
    """Factory class for Location model."""

    class Meta:  # noqa
        model = Location

    user = factory.SubFactory(UserF)
    name = factory.Faker('name')
    geometry = factory.LazyAttribute(
        lambda _: MultiPolygon(
            Polygon(((0, 0), (1, 0), (1, 1), (0, 1), (0, 0)))
        )
    )
    created_on = factory.Faker('date_time')


class APIRateLimiterFactory(DjangoModelFactory):
    """Factory class for APIRateLimiter model."""

    class Meta:  # noqa
        model = APIRateLimiter

    user = factory.SubFactory(UserF)
    minute_limit = 10
    hour_limit = 100
    day_limit = 1000


class UserFileFactory(DjangoModelFactory):
    """Factory class for UserFile momdel."""

    class Meta:  # noqa
        model = UserFile

    user = factory.SubFactory(UserF)
    name = 'dev/user_data/123.csv'
    query_params = {
        'product': 'cbam_historical_analysis_bias_adjust',
        'attributes': ['max_temperature'],
        'start_date': '2020-01-01',
        'end_date': '2020-01-02',
        'output_type': 'csv',
        'geom_type': 'point',
        'geometry': 'POINT (1 1)'
    }
