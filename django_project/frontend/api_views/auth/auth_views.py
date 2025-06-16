"""
Tomorrow Now GAP.

.. note:: authentication views.
"""
from django.core import signing
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework import status
from rest_framework.response import Response
import logging
logger = logging.getLogger(__name__)


class DecodeSocialSignupTokenView(APIView):
    """View to decode social signup token."""

    permission_classes = [AllowAny]

    def get(self, request):
        """Decode the social signup token."""
        token = request.query_params.get("token")
        if not token:
            return Response({}, status=204)

        try:
            data = signing.loads(token, salt="social-signup")
            return Response(data)
        except signing.BadSignature:
            return Response(
                {"detail": "Invalid token"},
                status=status.HTTP_400_BAD_REQUEST
            )
