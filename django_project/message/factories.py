# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Message
"""
import factory
from factory.django import DjangoModelFactory

from message.models import MessageTemplate
from prise.variables import PriseMessageGroup


class MessageTemplateFactory(DjangoModelFactory):
    """Factory class for MessageTemplate model."""

    class Meta:  # noqa
        model = MessageTemplate

    code = factory.Sequence(
        lambda n: f'code-{n}'
    )
    name = factory.Sequence(
        lambda n: f'name-{n}'
    )
    template = factory.Faker('text')
    group = PriseMessageGroup.START_SEASON
