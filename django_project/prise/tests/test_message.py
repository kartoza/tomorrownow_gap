# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Message Models.
"""

from django.test import TestCase

from gap.factories import PestFactory, FarmGroupFactory
from message.factories import MessageTemplateFactory
from prise.factories import PriseMessageFactory
from prise.models import PriseMessage, PriseMessagePestDoesNotExist
from prise.variables import PriseMessageGroup


class PriseMessageTest(TestCase):
    """PriseMessage test case."""

    def setUp(self):
        """Set SalientIngestorBaseTest."""
        self.pest_1 = PestFactory(name='pest 1')
        self.pest_2 = PestFactory(name='pest 2')
        self.pest_3 = PestFactory(name='pest 3')
        self.farm_group = FarmGroupFactory()

        message_1 = MessageTemplateFactory(
            template='This is template 1 in english with {{ language_code }}',
            template_sw=(
                'This is template 1 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        message_2 = MessageTemplateFactory(
            template='This is template 2 in english with {{ language_code }}',
            template_sw=(
                'This is template 2 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        message_3 = MessageTemplateFactory(
            template='This is template 3 in english with {{ language_code }}',
            template_sw=(
                'This is template 3 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.TIME_TO_ACTION_1
        )
        message_4 = MessageTemplateFactory(
            template='This is template 4 in english with {{ language_code }}',
            template_sw=(
                'This is template 4 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        message_5 = MessageTemplateFactory(
            template='This is template 5 in english with {{ language_code }}',
            template_sw=(
                'This is template 5 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        prise_message = PriseMessageFactory(pest=self.pest_1)
        prise_message.messages.add(*[message_1, message_2, message_3])
        prise_message = PriseMessageFactory(pest=self.pest_2)
        prise_message.messages.add(*[message_4])
        prise_message = PriseMessageFactory(
            pest=self.pest_1, farm_group=self.farm_group
        )
        prise_message.messages.add(*[message_5])

    def test_pest_no_message_objects(self):
        """Test return pest with no message."""
        with self.assertRaises(PriseMessagePestDoesNotExist):
            PriseMessage.get_messages_objects(self.pest_3)

    def test_pest_1_message_objects(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages_objects(self.pest_1).count(), 3
        )
        self.assertEqual(
            PriseMessage.get_messages_objects(
                self.pest_1, message_group=PriseMessageGroup.START_SEASON
            ).count(),
            2
        )
        self.assertEqual(
            PriseMessage.get_messages_objects(
                self.pest_1, message_group=PriseMessageGroup.TIME_TO_ACTION_1
            ).count(),
            1
        )

    def test_pest_2_message_objects(self):
        """Test return pest 2."""
        self.assertEqual(
            PriseMessage.get_messages_objects(self.pest_2).count(), 1
        )

    def test_pest_1_messages_en_start_season(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 1 in english with en',
                'This is template 2 in english with en',
            ]
        )

    def test_pest_1_messages_sw_start_season(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 1 in swahili with sw',
                'This is template 2 in swahili with sw',
            ]
        )

    def test_pest_1_messages_en_start_season_farm_group_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON,
                farm_group=self.farm_group
            ),
            [
                'This is template 5 in english with en'
            ]
        )

    def test_pest_1_messages_sw_start_season_farm_group_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON,
                farm_group=self.farm_group
            ),
            [
                'This is template 5 in swahili with sw'
            ]
        )

    def test_pest_1_messages_en_time_to_action_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.TIME_TO_ACTION_1
            ),
            [
                'This is template 3 in english with en'
            ]
        )

    def test_pest_1_messages_sw_time_to_action_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.TIME_TO_ACTION_1
            ),
            [
                'This is template 3 in swahili with sw',
            ]
        )

    def test_pest_2_messages_en(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_2,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 4 in english with en',
            ]
        )

    def test_pest_2_messages_sw(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_2,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 4 in swahili with sw',
            ]
        )

    def test_pest_3_messages_en(self):
        """Test return pest 1."""
        with self.assertRaises(PriseMessagePestDoesNotExist):
            PriseMessage.get_messages(
                self.pest_3,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON
            )

    def test_pest_3_messages_sw(self):
        """Test return pest 1."""
        with self.assertRaises(PriseMessagePestDoesNotExist):
            PriseMessage.get_messages(
                self.pest_3,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON
            )
