# coding=utf-8
"""Serializers for DCAS API."""

from rest_framework import serializers
from knox.models import AuthToken
from dcas.models.output import DCASOutput
from frontend.models import PagePermission, APIKeyMetadata


class OutputSerializer(serializers.ModelSerializer):
    """List representation of a CSV output."""

    class Meta:
        """Model serializer for DCASOutput with limited fields."""

        model = DCASOutput
        fields = ("id", "file_name", "size", "delivered_at")


class PermittedPageSerializer(serializers.ModelSerializer):
    """Serializer for permitted pages."""

    class Meta:
        """Meta options for PermittedPageSerializer."""

        model = PagePermission
        fields = ['page']


class APIKeySerializer(serializers.ModelSerializer):
    """Serializer for API keys, including metadata."""

    id = serializers.UUIDField(source="pk", read_only=True)
    token = serializers.CharField(read_only=True)
    name = serializers.CharField(source="metadata.name")
    description = serializers.CharField(
        allow_blank=True,
        required=False,
        source="metadata.description"
    )
    created = serializers.DateTimeField(read_only=True)
    expiry = serializers.DateTimeField(read_only=True)

    class Meta:
        """Meta options for APIKeySerializer."""

        model = AuthToken
        fields = (
            "id",
            "token",
            "name",
            "description",
            "created",
            "expiry",
        )

    def create(self, validated_data):
        """Create a new API key with metadata."""
        name = validated_data.pop("name")
        description = validated_data.pop("description", "")
        # create the Knox token
        instance, token = AuthToken.objects.create(
            user=self.context["request"].user,
            expiry=validated_data.get("expiry", None),
        )
        # store the extra metadata
        APIKeyMetadata.objects.create(
            token=instance,
            name=name,
            description=description,
        )
        # stash the plaintext for the response
        instance._plain_token = token
        return instance

    def to_representation(self, instance):
        """Convert the AuthToken instance to a dictionary representation."""
        # base serialization of AuthToken
        data = super().to_representation(instance)
        # inject the one-time plaintext token
        data["token"] = getattr(instance, "_plain_token", None)
        # override name/description from the related metadata
        meta = getattr(instance, "metadata", None)
        data["name"] = meta.name if meta else ""
        data["description"] = meta.description if meta else ""
        return data
