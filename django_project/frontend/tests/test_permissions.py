# coding=utf-8
"""Tomorrow Now GAP – tests for IsKalroUser permission."""

from django.test import TestCase
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, AnonymousUser
from rest_framework.test import APIRequestFactory

from frontend.permissions import IsKalroUser
from frontend.models import PagePermission


class TestIsKalroUser(TestCase):
    """Unit-tests for the custom DRF permission class."""

    @classmethod
    def setUpTestData(cls):
        """Set up test data for the permission tests."""
        cls.User = get_user_model()
        cls.factory = APIRequestFactory()

        # Groups
        cls.kalro_group, _ = Group.objects.get_or_create(name="Kalro")

        # Users
        cls.regular_user = cls.User.objects.create_user(
            username="regular_user",
            email="user@example.com", password="pass"
        )
        cls.kalro_user = cls.User.objects.create_user(
            username="kalro_user",
            email="kalro@example.com", password="pass"
        )
        cls.kalro_user.groups.add(cls.kalro_group)
        cls.super_user = cls.User.objects.create_superuser(
            username="super_user",
            email="root@example.com", password="pass"
        )
        cls.page_perm = PagePermission.objects.create(page="dcas_csv")
        cls.page_perm.groups.add(cls.kalro_group)

    def _has_perm(self, user, view=None):
        req = self.factory.get("/")
        req.user = user
        return IsKalroUser().has_permission(req, view)

    def test_anonymous_rejected(self):
        """Anonymous should fail immediately."""
        anon = AnonymousUser()
        self.assertFalse(self._has_perm(anon))

    def test_regular_user_rejected(self):
        """Authenticated but non‐Kalro, non‐admin should fail."""
        self.assertFalse(self._has_perm(self.regular_user))

    def test_kalro_user_allowed(self):
        """Member of Kalro group should pass."""
        self.assertTrue(self._has_perm(self.kalro_user))

    def test_super_user_allowed(self):
        """Super-user should pass regardless of page perms."""
        self.assertTrue(self._has_perm(self.super_user))
