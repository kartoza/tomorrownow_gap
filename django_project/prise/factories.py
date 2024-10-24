# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Message
"""
import factory
from factory.django import DjangoModelFactory

from gap.factories.crop_insight import PestFactory
from prise.models import PriseMessage


class PriseMessageFactory(DjangoModelFactory):
    """Factory class for PriseMessage model."""

    class Meta:  # noqa
        model = PriseMessage

    pest = factory.SubFactory(PestFactory)
