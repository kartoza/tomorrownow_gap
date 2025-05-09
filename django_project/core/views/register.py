"""Views for user registration and email verification."""

from rest_framework import generics
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from django.contrib.auth import get_user_model
from django.shortcuts import redirect
from django.utils.http import urlsafe_base64_decode
from django.contrib.auth.tokens import default_token_generator
from django.utils import timezone

from core.serializers import RegisterSerializer
from core.utils.token_gen import generate_verification_token
from core.utils.emails import send_verification_email
from gap.models import UserProfile

User = get_user_model()


class RegisterView(generics.CreateAPIView):
    """Register a new user."""

    serializer_class = RegisterSerializer
    permission_classes = [AllowAny]

    def perform_create(self, serializer):
        """Create a new user and send a verification email."""
        user = serializer.save()
        uid, token = generate_verification_token(user)
        send_verification_email(user, uid, token)


class VerifyEmailView(generics.GenericAPIView):
    """Verify a user's email."""

    permission_classes = [AllowAny]

    def get(self, request):
        """Verify a user's email."""
        uid = request.GET.get("uid")
        token = request.GET.get("token")

        if not uid or not token:
            return Response({"detail": "Missing parameters."}, status=400)

        try:
            user_id = urlsafe_base64_decode(uid).decode()
            user = User.objects.get(pk=user_id)
        except Exception:
            return Response({"detail": "Invalid token or user."}, status=400)

        if default_token_generator.check_token(user, token):
            user.save()
            profile = UserProfile.objects.get(user=user)
            profile.email_verified = True
            profile.verified_at = timezone.now()  # set at time of verification
            profile.save()
            return redirect(f"/signup-request/?uid={uid}&token={token}")
        return Response({"detail": "Invalid or expired token."}, status=400)
