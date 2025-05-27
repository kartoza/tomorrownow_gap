# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Test Service for Priority
"""

from unittest.mock import patch
from django.test import TestCase
from dcas.models import DCASMessagePriority, DCASConfig
from dcas.service import MessagePriorityService


def set_cache_dummy(cache_key, value, timeout):
    """Set cache mock."""
    pass


def set_cache_many_dummy(cache_key_map, timeout):
    """Set cache mock."""
    pass


class TestMessagePriorityService(TestCase):
    """Test cases for MessagePriorityService."""

    fixtures = [
        '1.object_storage_manager.json',
        '1.dcas_config.json'
    ]

    @patch('dcas.service.cache.get')
    @patch('dcas.service.cache.set', side_effect=set_cache_dummy)
    def test_get_priority_from_cache(self, mock_cache_set, mock_cache_get):
        """Test retrieving priority from cache."""
        mock_cache_get.return_value = 'High'
        service = MessagePriorityService()
        priority = service.get_priority('test_key', 1)
        self.assertEqual(priority, 'High')
        mock_cache_get.assert_called_once_with('message_priority:test_key:1')

    @patch('dcas.service.cache.get')
    @patch('dcas.service.cache.set', side_effect=set_cache_dummy)
    def test_get_priority_from_db_and_set_cache(
        self, mock_cache_set, mock_cache_get
    ):
        """Test retrieving priority from DB and setting it in cache."""
        mock_cache_get.return_value = None
        DCASMessagePriority.objects.create(
            code='test_key',
            priority=2,
            config=DCASConfig.objects.get(id=1)
        )
        service = MessagePriorityService()
        priority = service.get_priority('test_key', 1)
        self.assertEqual(priority, 2)
        mock_cache_get.assert_called_once_with('message_priority:test_key:1')
        mock_cache_set.assert_called_once_with(
            'message_priority:test_key:1',
            2,
            timeout=None
        )

    @patch('dcas.service.cache.get')
    def test_get_priority_key_not_found(self, mock_cache_get):
        """Test behavior when priority key is not found."""
        mock_cache_get.return_value = None
        service = MessagePriorityService()
        priority = service.get_priority('non_existent_key', 1)
        self.assertEqual(priority, 0)
        mock_cache_get.assert_called_once_with(
            'message_priority:non_existent_key:1'
        )

    @patch('dcas.service.cache.set_many', side_effect=set_cache_many_dummy)
    def test_load_priority_from_cache(self, mock_cache_set_many):
        """Test loading priority from cache."""
        DCASMessagePriority.objects.create(
            code='keytest1',
            priority=1,
            config=DCASConfig.objects.get(id=1)
        )
        service = MessagePriorityService()
        service.load_priority()
        mock_cache_set_many.assert_called_once_with({
            'message_priority:keytest1:1': 1
        }, timeout=None)

    @patch('dcas.service.cache.get')
    @patch('dcas.service.cache.set', side_effect=set_cache_dummy)
    def test_sort_messages_by_priority(self, mock_cache_set, mock_cache_get):
        """Test sorting messages by priority."""
        mock_cache_get.return_value = None
        DCASMessagePriority.objects.create(
            code='key1',
            priority=1,
            config=DCASConfig.objects.get(id=1)
        )
        DCASMessagePriority.objects.create(
            code='key2',
            priority=3,
            config=DCASConfig.objects.get(id=1)
        )
        DCASMessagePriority.objects.create(
            code='key3',
            priority=2,
            config=DCASConfig.objects.get(id=1)
        )
        service = MessagePriorityService()
        messages = ['key1', 'key2', 'key3']
        sorted_messages = service.sort_messages(messages, 1)
        self.assertEqual(
            [msg for msg in sorted_messages],
            ['key2', 'key3', 'key1']
        )
