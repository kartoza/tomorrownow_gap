# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message translation.
"""

from modeltranslation.translator import register, TranslationOptions

from message.models import MessageTemplate


@register(MessageTemplate)
class MessageTemplateTranslationOptions(TranslationOptions):
    """Translation options for MessageTemplate."""

    fields = ('template',)
