# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for TableUsage Model.
"""

from django.test import TestCase
from django.db import connection

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

    def test_clear_temp_table(self):
        """Test the clear_temp_table method."""
        # test create table in temp schema
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE SCHEMA IF NOT EXISTS temp;'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS temp.example_table '
                '(id SERIAL PRIMARY KEY);'
            )
            cursor.execute(
                'INSERT INTO temp.example_table (id) VALUES (1), (2), (3);'
            )

        # Create a temp table usage
        temp_table_usage = TableUsage.objects.create(
            schema_name='temp',
            data={'example_table': 'data'}
        )
        # Call the method
        TableUsage.clear_temp_table(temp_table_usage.id)
        # Check if the table is dropped
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT EXISTS (SELECT 1 FROM information_schema.tables '
                'WHERE table_name = %s);',
                ['example_table']
            )
            table_exists = cursor.fetchone()[0]
            self.assertFalse(table_exists)


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
