# coding=utf-8
"""Serializers for DCAS API."""

from rest_framework import serializers
from dcas.models.output import DCASOutput
from frontend.models import PagePermission


class OutputSerializer(serializers.ModelSerializer):
    """List representation of a CSV output."""

    class Meta:
        """Model serializer for DCASOutput with limited fields."""

        model = DCASOutput
        fields = ("id", "file_name", "size", "delivered_at")


class PermittedPageSerializer(serializers.ModelSerializer):
    """Serializer for permitted pages."""
    class Meta:
        model = PagePermission
        fields = ['page']
