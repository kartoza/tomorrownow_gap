# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User serializer class.
"""

from django.contrib.auth import get_user_model
from rest_framework import serializers

User = get_user_model()


class UserInfoSerializer(serializers.ModelSerializer):
    """Serializer for User Info."""

    class Meta:  # noqa
        model = User
        fields = [
            'id',
            'username',
            'email',
            'first_name',
            'last_name',
            'is_staff',
            'is_superuser',
        ]
        swagger_schema_fields = {
            'title': 'User Info',
            'example': {
                'username': 'jane.doe@example.com',
                'email': 'jane.doe@example.com',
                'first_name': 'Jane',
                'last_name': 'Doe'
            }
        }
