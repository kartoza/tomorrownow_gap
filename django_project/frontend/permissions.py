# coding=utf-8
"""
Custom DRF permission classes for Tomorrow-Now GAP.

Only KALRO staff (or super-users) may access certain DCAS resources.
"""

from rest_framework.permissions import BasePermission
from frontend.models import PagePermission


class IsKalroUser(BasePermission):
    """
    Allow requests.

    From:
    • super-users, OR
    • authenticated users who belong to the “KALRO” group.

    All other requests return HTTP 403.
    """

    message = "You must be a KALRO user to access this resource."
    permission_page = "dcas_csv"

    def has_permission(self, request, view):  # noqa: D401
        """Check if the user is a KALRO user."""
        user = request.user
        if not (user and user.is_authenticated):
            return False

        if user.is_superuser:
            return True

        page_key = getattr(view, 'permission_page', self.permission_page)

        try:
            perm = PagePermission.objects.get(page=page_key)
        except PagePermission.DoesNotExist:
            return False

        return perm.groups.filter(
            id__in=user.groups.values_list('id', flat=True)
        ).exists()

    def has_object_permission(self, request, view, obj):  # noqa: D401
        """Check if the user has permission for the object."""
        # Re-use the same logic for object-level checks
        return self.has_permission(request, view)
