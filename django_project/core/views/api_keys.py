"""
Tomorrow Now GAP.

.. note:: API Key Management View
"""

from datetime import timedelta
from knox.models import AuthToken
from knox.settings import knox_settings
from rest_framework import generics, permissions, status
from rest_framework.response import Response

from core.serializers import APIKeySerializer


class _Base(permissions.IsAuthenticated, generics.GenericAPIView):
    """Base class for API key views."""
    serializer_class = APIKeySerializer

    def get_queryset(self):
        """Return the queryset of API keys for the authenticated user. """
        return AuthToken.objects.filter(user=self.request.user)


class APIKeyListCreate(_Base, generics.ListCreateAPIView):
    """
    GET - list current keys.

    POST - create a new key, return plaintext once.
    """

    def create(self, request, *args, **kwargs):
        """Create a new API key for the authenticated user."""
        expiry = knox_settings.TOKEN_TTL or timedelta(days=365)
        instance, token = AuthToken.objects.create(
            user=request.user, expiry=expiry
        )
        headers = self.get_success_headers({})
        return Response(
            {
                "id": instance.pk,
                "token": token,
                "created": instance.created,
                "expiry": instance.expiry,
            },
            headers=headers,
            status=201,
        )


class APIKeyDestroy(_Base, generics.DestroyAPIView):
    """DELETE - revoke an API key by ID. """

    lookup_field = "pk"

    def destroy(self, request, *args, **kwargs):
        """Revoke an API key by ID."""
        super().destroy(request, *args, **kwargs)
        return Response(
            {"detail": "API key revoked"},
            status=status.HTTP_200_OK
        )
