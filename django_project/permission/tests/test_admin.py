# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Admin.
"""

from django.contrib import admin
from django.contrib.auth.models import Permission
from django.test import TestCase, RequestFactory
from django.contrib.auth.models import Group

from core.factories import UserF
from gap.models import DatasetType
from permission.models import (
    DatasetTypeGroupObjectPermission,
    DatasetTypeUserObjectPermission,
    PermissionType
)
from permission.admin import (
    DatasetTypeGroupObjectPermissionAdmin,
    DatasetTypeUserObjectPermissionAdmin
)


class DatasetTypeObjectPermissionAdminTestCase(TestCase):
    """Test class for Permission Admin."""

    fixtures = [
        '4.dataset_type.json'
    ]

    @classmethod
    def setUpTestData(cls):
        """Set the test data."""
        cls.user = UserF.create(
            is_superuser=False
        )
        cls.group = Group.objects.create(name='test_group')
        cls.type1 = DatasetType.objects.get(
            variable_name='tahmo_ground_observation'
        )
        # Create the required permission
        cls.view_permission = Permission.objects.get(
            codename=PermissionType.VIEW_DATASET_TYPE
        )

        # Initialize a request factory and sample objects
        cls.factory = RequestFactory()
        cls.group_permission = (
            DatasetTypeGroupObjectPermission.objects.create(
                group_id=cls.group.id,
                content_object_id=cls.type1.id,
                permission=cls.view_permission
            )
        )
        cls.user_permission = (
            DatasetTypeUserObjectPermission.objects.create(
                user_id=cls.user.id,
                content_object_id=cls.type1.id,
                permission=cls.view_permission
            )
        )

    def test_group_admin_get_form(self):
        """Test GetForm in group object permission."""
        admin_instance = DatasetTypeGroupObjectPermissionAdmin(
            model=DatasetTypeGroupObjectPermission, admin_site=admin.site
        )
        request = self.factory.get(
            "/admin/permission/datasettypegroupobjectpermission/"
        )
        request.user = self.user
        form = admin_instance.get_form(request, obj=self.group_permission)()

        # Assert that the 'permission' field is disabled
        self.assertTrue(form.base_fields['permission'].disabled)

        # Assert that the initial value is set to the correct permission
        self.assertEqual(
            form.base_fields['permission'].initial,
            self.view_permission
        )

    def test_user_admin_get_form(self):
        """Test GetForm in user object permission."""
        admin_instance = DatasetTypeUserObjectPermissionAdmin(
            model=DatasetTypeUserObjectPermission, admin_site=admin.site
        )
        request = self.factory.get(
            "/admin/permission/datasettypeuserobjectpermission/"
        )
        request.user = self.user
        form = admin_instance.get_form(request, obj=self.user_permission)()

        # Assert that the 'permission' field is disabled
        self.assertTrue(form.base_fields['permission'].disabled)

        # Assert that the initial value is set to the correct permission
        self.assertEqual(
            form.base_fields['permission'].initial,
            self.view_permission
        )
