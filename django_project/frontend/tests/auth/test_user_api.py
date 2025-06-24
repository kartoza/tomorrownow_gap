# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

from django.urls import reverse
from django.contrib.auth.models import Group

from core.tests.common import BaseAPIViewTest
from frontend.api_views.auth.user import UserInfoAPI
from frontend.models import PagePermission


class TestUserInfoAPI(BaseAPIViewTest):
    """Test case for User Info API."""

    def setUp(self):
        """Init test class."""
        super().setUp()
        self.url = reverse('user-info')
        self.view = UserInfoAPI.as_view()

    def test_empty_page_permissions(self):
        """Test user info with no page permissions."""
        request = self.factory.get(self.url)
        request.user = self.user_1
        response = self.view(request)
        self.assertEqual(response.status_code, 200)
        data = response.data
        self.assertIn('username', data)
        self.assertEqual(data['username'], self.user_1.username)
        self.assertIn('email', data)
        self.assertEqual(data['email'], self.user_1.email)
        self.assertIn('pages', data)
        self.assertEqual(data['pages'], [])

    def test_user_info_with_page_permissions(self):
        """Test user info with page permissions."""
        group = Group.objects.create(name='test_group')
        self.user_1.groups.add(group)
        # Create a page permission for the user
        page_perm = PagePermission.objects.create(
            page='test_page'
        )
        page_perm.groups.add(group)

        request = self.factory.get(self.url)
        request.user = self.user_1
        response = self.view(request)
        self.assertEqual(response.status_code, 200)
        data = response.data
        self.assertIn('username', data)
        self.assertEqual(data['username'], self.user_1.username)
        self.assertIn('email', data)
        self.assertEqual(data['email'], self.user_1.email)
        self.assertIn('pages', data)
        self.assertEqual(data['pages'], ['test_page'])

    def test_superuser_with_page_permissions(self):
        """Test user info with page permissions."""
        group = Group.objects.create(name='test_group')
        # Create a page permission for the user
        page_perm = PagePermission.objects.create(
            page='test_page'
        )
        page_perm.groups.add(group)

        request = self.factory.get(self.url)
        request.user = self.superuser
        response = self.view(request)
        self.assertEqual(response.status_code, 200)
        data = response.data
        self.assertIn('username', data)
        self.assertEqual(data['username'], self.superuser.username)
        self.assertIn('email', data)
        self.assertEqual(data['email'], self.superuser.email)
        self.assertIn('pages', data)
        self.assertEqual(data['pages'], ['test_page'])
