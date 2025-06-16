# coding=utf-8
"""
Serializers for DCAS API.
"""

from rest_framework import serializers
from dcas.models.output import DCASOutput


class OutputSerializer(serializers.ModelSerializer):
    """Lightweight list representation of a CSV output."""

    class Meta:
        """Model serializer for DCASOutput with limited fields."""
        model = DCASOutput
        fields = ("id", "file_name", "size", "delivered_at")
