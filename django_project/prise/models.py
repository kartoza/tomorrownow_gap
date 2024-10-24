# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message prise models.
"""

from django.db import models

from gap.models.crop_insight import Pest
from gap.models.farm_group import FarmGroup
from message.models import MessageTemplate


class PriseMessagePestDoesNotExist(Exception):
    """Prise message of pest does not exist exception."""

    def __init__(self, pest: Pest):  # noqa
        self.message = (
            f'Prise message with pest {pest.name} does not exist.'
        )
        super().__init__(self.message)


class PriseMessage(models.Model):
    """Model that stores message template linked with pest."""

    pest = models.ForeignKey(
        Pest, on_delete=models.CASCADE
    )
    farm_group = models.ForeignKey(
        FarmGroup, on_delete=models.CASCADE,
        help_text='A default message if farm group is not specified.',
        null=True, blank=True
    )
    messages = models.ManyToManyField(
        MessageTemplate, blank=True
    )

    def __str__(self):
        """Return string representation."""
        return self.pest.name

    class Meta:  # noqa
        unique_together = ('pest', 'farm_group')
        ordering = ('pest__name',)
        db_table = 'prise_message'

    @staticmethod
    def get_messages_objects(
            pest: Pest, message_group: str = None, farm_group: FarmGroup = None
    ):
        """Return message objects."""
        try:
            message = PriseMessage.objects.get(
                pest=pest, farm_group=farm_group
            ).messages.all()
            if message_group:
                message = message.filter(group=message_group)
            return message
        except PriseMessage.DoesNotExist:
            raise PriseMessagePestDoesNotExist(pest)

    @staticmethod
    def get_messages(
            pest: Pest, message_group: str, context=dict,
            language_code: str = None, farm_group: FarmGroup = None

    ):
        """Return messages string."""
        return [
            message.get_message(context, language_code)
            for message in PriseMessage.get_messages_objects(
                pest=pest, message_group=message_group, farm_group=farm_group
            )
        ]
