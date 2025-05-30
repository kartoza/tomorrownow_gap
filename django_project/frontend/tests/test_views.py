# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for Views.
"""

from django.test import Client

from core.tests.common import BaseAPIViewTest


class TestHomeView(BaseAPIViewTest):
    """Test HomeView class."""

    def test_home_view(self):
        """Test Home View."""
        c = Client()
        response = c.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.has_header('Content-Type'))
        self.assertIn('text/html', response.headers['Content-Type'])
        self.assertIn('gap_base_context', response.context)
