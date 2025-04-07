"""Sign-up access requests."""

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from django.contrib.auth import get_user_model
from core.serializers import SignUpRequestSerializer
from gap.models import UserProfile


User = get_user_model()


class SignUpRequestView(APIView):
    """Handles POST sign-up access requests."""

    permission_classes = [AllowAny]

    def post(self, request):
        """Handle POST requests."""
        data = request.data
        email = data.get('email')

        # Check that email is provided
        if not email:
            return Response(
                {"detail": "Email is required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Lookup user by email and ensure their email is verified
        try:
            user = User.objects.get(email=email)
            profile = UserProfile.objects.get(user=user)
        except (User.DoesNotExist, UserProfile.DoesNotExist):
            return Response(
                {"detail": "Invalid user or profile."},
                status=status.HTTP_403_FORBIDDEN
            )

        if not profile.email_verified:
            message = (
                "Your email is not verified. "
                "Please verify your email before submitting this request."
            )
            return Response(
                {
                    "detail": message,
                },
                status=status.HTTP_403_FORBIDDEN
            )

        serializer = SignUpRequestSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(
                {"detail": "Request submitted successfully."},
                status=status.HTTP_200_OK
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
