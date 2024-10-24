# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Message Models.
"""

from django.test import TestCase

from message.factories import MessageTemplateFactory
from message.models import MessageLanguageNotSupportedException


class MessageTemplateTest(TestCase):
    """MessageTemplate test case."""

    def test_get_template_language_not_found(self):
        """Test create object."""
        message = MessageTemplateFactory()

        with self.assertRaises(MessageLanguageNotSupportedException):
            message.get_message(language_code='af')

    def test_get_template_context_not_found(self):
        """Test create object."""
        message = MessageTemplateFactory(
            template='This text with {{ language_code }}',
        )
        self.assertEqual(message.get_message(), 'This text with ')

    def test_get_template_context(self):
        """Test create object."""
        message = MessageTemplateFactory(
            template='This text with {{ language_code }}',
        )
        self.assertEqual(
            message.get_message(
                language_code='en', context={
                    'language_code': 'en'
                }
            ),
            'This text with en'
        )

    def test_get_template_context_with_different_language(self):
        """Test create object."""
        message = MessageTemplateFactory(
            template='This text with {{ language_code }} in english',
            template_sw='This text with {{ language_code }} in swahili',
        )
        self.assertEqual(
            message.get_message(
                language_code='sw',
                context={
                    'language_code': 'sw'
                }
            ),
            'This text with sw in swahili'
        )
