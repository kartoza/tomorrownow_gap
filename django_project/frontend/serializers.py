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
    """Serializer for API keys."""

    id = serializers.UUIDField(source="pk", read_only=True)
    token = serializers.CharField(read_only=True)
    name = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()
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

    def get_name(self, instance):
        """Retrieve the name of the API key from metadata."""
        try:
            meta = APIKeyMetadata.objects.get(digest=instance.digest)
            return meta.name
        except APIKeyMetadata.DoesNotExist:
            return ""

    def get_description(self, instance):
        """Retrieve the description of the API key from metadata."""
        try:
            meta = APIKeyMetadata.objects.get(digest=instance.digest)
            return meta.description
        except APIKeyMetadata.DoesNotExist:
            return ""

    def create(self, validated_data):
        """Create a new Knox token and its metadata."""
        request = self.context["request"]
        name = request.data.get("name", "")
        description = request.data.get("description", "")
        expiry = validated_data.get("expiry", None)

        instance, token = AuthToken.objects.create(
            user=request.user,
            expiry=expiry,
        )
        APIKeyMetadata.objects.create(
            digest=instance.digest,
            name=name,
            description=description,
        )
        instance._plain_token = token
        return instance

    def to_representation(self, instance):
        """Customize representation to include raw token only on creation."""
        data = super().to_representation(instance)
        # only include the raw token immediately after creation
        if not hasattr(instance, "_plain_token"):
            data.pop("token", None)
        return data
