# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS CSV Permissions
"""


from django.db import models
from django.utils.translation import gettext_lazy as _
from guardian.models import UserObjectPermissionBase
from guardian.models import GroupObjectPermissionBase

from gap.models import Country


class DCASPermissionType:
    """Enum that represents the permission type."""

    VIEW_DCAS_OUTPUT_COUNTRY = 'view_dcasoutput_country'


class DCASCountryUserObjectPermission(UserObjectPermissionBase):
    """Model for storing DCAS Country Object Permission by user."""

    content_object = models.ForeignKey(Country, on_delete=models.CASCADE)

    class Meta(UserObjectPermissionBase.Meta):  # noqa
        db_table = 'permission_dcas_country_user'
        verbose_name = _('User DCAS Country Permission')


class DCASCountryGroupObjectPermission(GroupObjectPermissionBase):
    """Model for storing DCAS Country Object Permission by group."""

    content_object = models.ForeignKey(Country, on_delete=models.CASCADE)

    class Meta(GroupObjectPermissionBase.Meta):  # noqa
        db_table = 'permission_dcas_country_group'
        verbose_name = _('Group DCAS Country Permission')
