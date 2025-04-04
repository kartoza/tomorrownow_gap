# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Farm Models.
"""
import factory
from django.contrib.gis.geos import Point
from factory.django import DjangoModelFactory

from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm, FarmRegistry,
    FarmRegistryGroup
)
from gap.models.farm_group import FarmGroup


class FarmCategoryFactory(DjangoModelFactory):
    """Factory class for FarmCategory model."""

    class Meta:  # noqa
        model = FarmCategory

    name = factory.Sequence(
        lambda n: f'farm-category-{n}'
    )
    description = factory.Faker('text')


class FarmRSVPStatusFactory(DjangoModelFactory):
    """Factory class for FarmRSVPStatus model."""

    class Meta:  # noqa
        model = FarmRSVPStatus

    name = factory.Sequence(
        lambda n: f'farm-rsvp-status-{n}'
    )
    description = factory.Faker('text')


class FarmFactory(DjangoModelFactory):
    """Factory class for Farm model."""

    class Meta:  # noqa
        model = Farm

    unique_id = factory.Sequence(
        lambda n: f'id-{n}'
    )
    geometry = factory.LazyFunction(lambda: Point(0, 0))
    rsvp_status = factory.SubFactory(FarmRSVPStatusFactory)
    category = factory.SubFactory(FarmCategoryFactory)
    crop = factory.SubFactory('gap.factories.crop_insight.CropFactory')
    phone_number = '123-456-7936'


class FarmGroupFactory(DjangoModelFactory):
    """Factory class for FarmGroup model."""

    class Meta:  # noqa
        model = FarmGroup

    name = factory.Sequence(lambda n: f'name-{n}')


class FarmRegistryGroupFactory(DjangoModelFactory):
    """Factory class for FarmRegistryGroup model."""

    class Meta:  # noqa
        model = FarmRegistryGroup

    name = factory.Sequence(lambda n: f'name-{n}')
    date_time = factory.Faker('date')


class FarmRegistryFactory(DjangoModelFactory):
    """Factory class for FarmRegistry model."""

    class Meta:  # noqa
        model = FarmRegistry

    group = factory.SubFactory(FarmRegistryGroupFactory)
    farm = factory.SubFactory(FarmFactory)
    crop = factory.SubFactory('gap.factories.crop_insight.CropFactory')
    crop_stage_type = factory.SubFactory(
        'gap.factories.crop_insight.CropStageTypeFactory'
    )
    planting_date = factory.Faker('date')
