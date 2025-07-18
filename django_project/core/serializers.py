"""Serializers for the core app."""

from django.contrib.auth import get_user_model
from rest_framework import serializers
from django.contrib.auth.password_validation import validate_password
from gap.models import SignUpRequest

User = get_user_model()


class RegisterSerializer(serializers.ModelSerializer):
    """Register a new user."""

    password = serializers.CharField(
        write_only=True, validators=[validate_password]
    )
    confirm_password = serializers.CharField(write_only=True)

    class Meta:
        """Meta class for RegisterSerializer."""

        model = User
        fields = [
            "email", "password", "confirm_password"
        ]
        extra_kwargs = {
            'email': {'required': True},
            'password': {'write_only': True, 'required': True},
            'confirm_password': {'write_only': True, 'required': True}

        }

    def validate_email(self, value):
        """Validate the email field."""
        print(f"Validating email: {value}")
        if User.objects.filter(email=value).exists():
            raise serializers.ValidationError(
                "This email is already registered."
            )
        return value

    def validate(self, data):
        """Validate the password and confirm_password fields."""
        if data["password"] != data["confirm_password"]:
            raise serializers.ValidationError("Passwords do not match.")
        return data

    def create(self, validated_data):
        """Create a new user."""
        validated_data.pop("confirm_password")
        user = User.objects.create_user(
            email=validated_data["email"],
            username=validated_data["email"],
            password=validated_data["password"],
            is_active=False,  # Email verification required
        )
        return user


class SignUpRequestSerializer(serializers.ModelSerializer):
    """Serializer for sign-up requests."""

    class Meta:
        """Meta class for SignUpRequestSerializer."""

        model = SignUpRequest
        fields = [
            'first_name',
            'last_name',
            'email',
            'organization',
            'description'
        ]
