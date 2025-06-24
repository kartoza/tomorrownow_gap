# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User APIs
"""

from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from frontend.models import PagePermission
from gap_api.serializers.user import UserInfoSerializer


class UserInfoAPI(APIView):
    """API to return user info."""

    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        """Login user info.

        Return current login user information.
        """
        data = UserInfoSerializer(request.user).data
        # add page_permissions
        data['pages'] = PagePermission.get_page_permissions(request.user)
        return Response(
            status=200, data=data
        )
