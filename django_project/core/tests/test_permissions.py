# coding=utf-8
"""Tomorrow Now GAP – tests for IsKalroUser permission."""

from django.test import TestCase
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from rest_framework.test import APIRequestFactory

from core.permissions import IsKalroUser


class TestIsKalroUser(TestCase):
    """Unit-tests for the custom DRF permission class."""

    @classmethod
    def setUpTestData(cls):
        """Set up test data for the permission tests."""
        cls.User = get_user_model()
        cls.factory = APIRequestFactory()

        # Groups
        cls.kalro_group, _ = Group.objects.get_or_create(name="KALRO")

        # Users
        cls.regular_user = cls.User.objects.create_user(
            email="user@example.com", password="pass"
        )
        cls.kalro_user = cls.User.objects.create_user(
            email="kalro@example.com", password="pass"
        )
        cls.kalro_user.groups.add(cls.kalro_group)
        cls.super_user = cls.User.objects.create_superuser(
            email="root@example.com", password="pass"
        )

    def _has_perm(self, user):
        """Return permission result for a GET request."""
        request = self.factory.get("/")
        request.user = user
        return IsKalroUser().has_permission(request, view=None)

    def test_anonymous_rejected(self):
        """Unauthenticated user should fail."""
        request = self.factory.get("/")
        request.user = self.User()  # empty, not authenticated
        self.assertFalse(IsKalroUser().has_permission(request, None))

    def test_regular_user_rejected(self):
        """Authenticated but non-KALRO, non-admin should fail."""
        self.assertFalse(self._has_perm(self.regular_user))

    def test_kalro_user_allowed(self):
        """Member of KALRO group should pass."""
        self.assertTrue(self._has_perm(self.kalro_user))

    def test_super_user_allowed(self):
        """Super-user should pass regardless of groups."""
        self.assertTrue(self._has_perm(self.super_user))
