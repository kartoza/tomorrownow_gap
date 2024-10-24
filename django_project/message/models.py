# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message models.
"""

from django.conf import settings
from django.db import models
from django.template import Template, Context
from django.utils.translation import gettext_lazy as _

from prise.variables import PriseMessageGroup


class MessageLanguageNotSupportedException(Exception):
    """Message language not supported exception."""

    def __init__(self):  # noqa
        self.message = (
            f'The language is not supported. '
            f'Choices: {[lang[0] for lang in settings.LANGUAGES]}'' '
        )
        super().__init__(self.message)


class MessageApplication:
    """The application that will use the message."""

    PRISE = 'PRISE'  # Message that will be used for CABI PRISE


class MessageTemplate(models.Model):
    """Model that stores message template by group and application."""

    code = models.CharField(
        max_length=512, unique=True
    )
    name = models.CharField(
        max_length=512
    )
    type = models.CharField(
        blank=True, null=True, max_length=512
    )
    application = models.CharField(
        default=MessageApplication.PRISE,
        choices=(
            (MessageApplication.PRISE, _(MessageApplication.PRISE)),
        ),
        max_length=512
    )
    group = models.CharField(
        default=PriseMessageGroup.START_SEASON,
        choices=(
            (
                PriseMessageGroup.START_SEASON,
                _(PriseMessageGroup.START_SEASON)
            ),
            (
                PriseMessageGroup.TIME_TO_ACTION_1,
                _(PriseMessageGroup.TIME_TO_ACTION_1)
            ),
            (
                PriseMessageGroup.TIME_TO_ACTION_2,
                _(PriseMessageGroup.TIME_TO_ACTION_2)
            ),
            (
                PriseMessageGroup.END_SEASON,
                _(PriseMessageGroup.END_SEASON)
            ),
        ),
        max_length=512
    )
    note = models.TextField(
        blank=True, null=True,
        help_text=_(
            'Just a note about the template.'
        )
    )
    template = models.TextField(
        help_text=_(
            'Field for storing messages in translation. '
            'Include {{ context_key }} as a placeholder '
            'to be replaced with the appropriate context.'
        )
    )

    class Meta:  # noqa
        ordering = ('code',)
        db_table = 'message_template'
        indexes = [
            models.Index(fields=['group']),
            models.Index(fields=['application']),
            models.Index(fields=['name']),
        ]

    def __str__(self):
        """Return string representation of MessageTemplate."""
        return self.code

    def get_message(self, context=dict, language_code: str = None):
        """Return template by language code.

        Also auto assign the data from context to template.
        """
        if not language_code:
            language_code = settings.LANGUAGES[0][0]
        try:
            template = Template(
                getattr(self, f'template_{language_code}')
            )
            context_obj = Context(context)
            return template.render(context_obj)
        except AttributeError:
            raise MessageLanguageNotSupportedException()
