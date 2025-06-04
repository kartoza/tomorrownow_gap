# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Permission.
"""

from django.test import TestCase
from guardian.shortcuts import assign_perm, remove_perm
from django.contrib.auth.models import Group

from core.factories import UserF
from gap.models import DatasetType
from permission.models import PermissionType


class TestPermission(TestCase):
    """Test class for Dataset Type access permission."""

    fixtures = [
        '4.dataset_type.json'
    ]

    def setUp(self):
        """Set the permission test."""
        self.superuser = UserF.create(
            is_superuser=True
        )
        self.user = UserF.create(
            is_superuser=False
        )
        self.type1 = DatasetType.objects.get(
            variable_name='tahmo_ground_observation'
        )

    def test_group_permission(self):
        """Test permission by group."""
        group = Group.objects.create(name='test_group')
        self.user.groups.add(group)

        self.assertFalse(
            self.user.has_perm(PermissionType.VIEW_DATASET_TYPE, self.type1)
        )

        assign_perm(PermissionType.VIEW_DATASET_TYPE, group, self.type1)

        self.assertTrue(
            self.user.has_perm(PermissionType.VIEW_DATASET_TYPE, self.type1)
        )

        # remove perm
        remove_perm(PermissionType.VIEW_DATASET_TYPE, group, self.type1)
        self.assertFalse(
            self.user.has_perm(PermissionType.VIEW_DATASET_TYPE, self.type1)
        )

    def test_user_permission(self):
        """Test permission by user."""
        self.assertFalse(
            self.user.has_perm(PermissionType.VIEW_DATASET_TYPE, self.type1)
        )

        assign_perm(PermissionType.VIEW_DATASET_TYPE, self.user, self.type1)

        self.assertTrue(
            self.user.has_perm(PermissionType.VIEW_DATASET_TYPE, self.type1)
        )

        # remove perm
        remove_perm(PermissionType.VIEW_DATASET_TYPE, self.user, self.type1)
        self.assertFalse(
            self.user.has_perm(PermissionType.VIEW_DATASET_TYPE, self.type1)
        )

        # superuser always has access
        self.assertTrue(
            self.superuser.has_perm(
                PermissionType.VIEW_DATASET_TYPE, self.type1
            )
        )
