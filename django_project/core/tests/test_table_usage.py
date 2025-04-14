# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for TableUsage Model.
"""

from django.test import TestCase

from core.models.table_usage import TableUsage
from django.contrib.admin.sites import AdminSite
from core.admin import TableUsageAdmin


class TableUsageModelTest(TestCase):
    """Unit tests for TableUsage Model."""

    def setUp(self):
        """Set up test data."""
        self.table_usage = TableUsage.objects.create(
            schema_name='public',
            data={'example': 'data'}
        )

    def test_table_usage_creation(self):
        """Test that a TableUsage object is created successfully."""
        self.assertEqual(self.table_usage.schema_name, 'public')
        self.assertIsNotNone(self.table_usage.created_on)
        self.assertEqual(self.table_usage.data, {'example': 'data'})

    def test_get_table_stats_for_schema(self):
        """Test the get_table_stats_for_schema method."""
        updated_table_usage = TableUsage.get_table_stats_for_schema(
            self.table_usage.id
        )

        # Assertions
        self.assertEqual(updated_table_usage.schema_name, 'public')
        self.assertIn('datetime', updated_table_usage.data)
        self.assertIn('core_tableusage', updated_table_usage.data)
        self.assertEqual(
            updated_table_usage.data['core_tableusage']['row_count'],
            1
        )


class MockRequest:
    """Mock request object for testing."""

    pass


class TableUsageAdminTest(TestCase):
    """Unit tests for TableUsageAdmin."""

    def setUp(self):
        """Set up test data."""
        self.site = AdminSite()
        self.admin = TableUsageAdmin(TableUsage, self.site)
        self.table_usage = TableUsage.objects.create(
            schema_name='public',
            data={'example': 'data'}
        )
        self.request = MockRequest()

    def test_display_fields(self):
        """Test that the list display fields are correct."""
        self.assertEqual(
            self.admin.get_list_display(self.request),
            ('schema_name', 'created_on')
        )

    def test_search_fields(self):
        """Test that the search fields are correct."""
        self.assertEqual(
            self.admin.get_search_fields(self.request),
            ('schema_name',)
        )

    def test_queryset(self):
        """Test that the queryset is filtered correctly."""
        queryset = self.admin.get_queryset(self.request)
        self.assertIn(self.table_usage, queryset)
