"""
Tomorrow Now GAP.

.. note:: API Key Management View
"""

from datetime import timedelta, datetime
from django.utils.dateparse import parse_date
from django.utils import timezone
from knox.models import AuthToken
from knox.settings import knox_settings
from rest_framework import generics, permissions, status
from rest_framework.response import Response

from frontend.serializers import APIKeySerializer
from frontend.models import APIKeyMetadata


class _Base(permissions.IsAuthenticated, generics.GenericAPIView):
    """Base class for API key views."""

    serializer_class = APIKeySerializer

    def get_queryset(self):
        """Return the queryset of API keys for the authenticated user."""
        return AuthToken.objects.filter(user=self.request.user)


class APIKeyListCreate(_Base, generics.ListCreateAPIView):
    """
    GET - list current keys.

    POST - create a new key, return plaintext once.
    """

    def create(self, request, *args, **kwargs):
        """Create a new API key for the authenticated user."""
        name = request.data.get("name", "")
        description = request.data.get("description", "")
        expiry_input = request.data.get("expiry")
        if expiry_input:
            d = parse_date(expiry_input)
            abs_dt = timezone.make_aware(
                datetime.combine(d, datetime.min.time())
            )
            now = timezone.now()
            ttl = abs_dt - now
            expiry = ttl if ttl > timedelta(seconds=0) else timedelta(0)
        else:
            expiry = knox_settings.TOKEN_TTL or timedelta(days=365)

        instance, token = AuthToken.objects.create(
            user=request.user,
            expiry=expiry
        )

        # Create metadata for the API key
        APIKeyMetadata.objects.create(
            digest = instance.digest,
            name = name,
            description = description,
        )

        headers = self.get_success_headers({})
        return Response(
            {
                "id": instance.pk,
                "token": token,
                "name": name,
                "description": description,
                "created": instance.created,
                "expiry": instance.expiry,
            },
            headers=headers,
            status=201,
        )


class APIKeyDestroy(_Base, generics.DestroyAPIView):
    """DELETE - revoke an API key by ID."""

    lookup_field = "pk"
    lookup_url_kwarg = "key_id"

    def destroy(self, request, *args, **kwargs):
        """Revoke an API key by ID."""
        instance = self.get_object()
        digest = instance.digest

        # revoke the Knox token
        instance.delete()

        # remove our metadata record
        APIKeyMetadata.objects.filter(digest=digest).delete()
        return Response(
            {"detail": "API key revoked"},
            status=status.HTTP_200_OK
        )
