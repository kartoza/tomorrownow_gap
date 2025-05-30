"""Sign-up access requests."""

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny, IsAuthenticated
from django.contrib.auth import get_user_model
from django.utils.http import urlsafe_base64_decode
from core.serializers import SignUpRequestSerializer
from gap.models import UserProfile, SignUpRequest


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


class UserFromUIDView(APIView):
    """Get user details from UID."""

    permission_classes = [AllowAny]

    def get(self, request, uid):
        """Get user details from UID."""
        try:
            user_id = urlsafe_base64_decode(uid).decode()
            user = User.objects.get(pk=user_id)
            return Response({
                "email": user.email,
            })
        except Exception as e:
            return Response({"detail": str(e)}, status=400)


class SignUpRequestStatusView(APIView):
    """
    Checks whether a user has already submitted a signup request
    or if one is still required.
    """

    permission_classes = [AllowAny]

    def get(self, request):
        """Check if the signup request form should be shown."""
        email = request.query_params.get("email")

        if not email:
            return Response({"show_form": False})

        try:
            User.objects.get(email=email)
        except User.DoesNotExist:
            return Response({"show_form": False})

        # If a signup request already exists and is pending/approved,
        # no need to show form
        existing = SignUpRequest.objects.filter(email=email).first()
        if existing:
            return Response({"show_form": False})

        # Otherwise, allow form display
        return Response({"show_form": True})


class CurrentUserView(APIView):
    """Returns the current user's details."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get current user details."""

        user = request.user
        return Response({
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "email_verified": getattr(
                user.userprofile, 'email_verified', False
            ),
        })


class MySignUpRequestView(APIView):
    """Returns the sign-up request for the current user."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get the sign-up request for the current user."""
        try:
            signup_request = SignUpRequest.objects.get(
                email=request.user.email
            )
            serializer = SignUpRequestSerializer(signup_request)
            return Response(serializer.data)
        except SignUpRequest.DoesNotExist:
            return Response({'detail': 'Not found'}, status=404)
