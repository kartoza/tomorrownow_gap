# coding=utf-8
"""
Custom DRF permission classes for Tomorrow-Now GAP.

Only KALRO staff (or super-users) may access certain DCAS resources.
"""

from rest_framework.permissions import BasePermission


class IsKalroUser(BasePermission):
    """
    Allow requests.

    From:
    • super-users, OR
    • authenticated users who belong to the “KALRO” group.

    All other requests return HTTP 403.
    """

    message = "You must be a KALRO user to access this resource."

    def has_permission(self, request, view):  # noqa: D401
        user = request.user
        if not (user and user.is_authenticated):
            return False

        if user.is_superuser:
            return True

        # NB: .exists() avoids loading the full queryset
        return user.groups.filter(name="KALRO").exists()

    def has_object_permission(self, request, view, obj):  # noqa: D401
        # Re-use the same logic for object-level checks
        return self.has_permission(request, view)
