"""Sign-up access requests."""

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny, IsAuthenticated
from django.contrib.auth import get_user_model
from django.utils.http import urlsafe_base64_decode
from django.utils import timezone
from allauth.account.utils import send_email_confirmation

from core.serializers import SignUpRequestSerializer
from gap.models import UserProfile, SignUpRequest, RequestStatus
from django.core.validators import validate_email
from django.core.exceptions import ValidationError


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
        # Validate email format
        try:
            validate_email(email)
        except ValidationError:
            return Response(
                {"detail": "Invalid email format."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Check if user already exists and is active
        if User.objects.filter(email=email, is_active=True).exists():
            return Response(
                {"detail": "User with this email already exists."},
                status=status.HTTP_400_BAD_REQUEST
            )
        # Validate and save the sign-up request
        if not data.get('first_name') or not data.get('last_name'):
            return Response(
                {"detail": "First name and last name are required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not data.get('organization'):
            return Response(
                {"detail": "Organization is required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not data.get('description'):
            return Response(
                {"detail": "Description is required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        existing_req = SignUpRequest.objects.filter(
            email=email
        ).first()
        if existing_req and existing_req.status != RequestStatus.PENDING:
            return Response(
                {"detail": "An approved/rejected request already exists."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        serializer = SignUpRequestSerializer(
            instance=existing_req,
            data=request.data
        )
        if serializer.is_valid():
            valid = serializer.validated_data

            # upsert the SignUpRequest so duplicate submits just update it
            req, created = SignUpRequest.objects.update_or_create(
                email=valid["email"],
                status=RequestStatus.PENDING,
                defaults={
                    "first_name": valid["first_name"],
                    "last_name": valid["last_name"],
                    "organization": valid["organization"],
                    "description": valid["description"],
                    "submitted_at": timezone.now(),
                },
            )

            # ensure there’s a User record (inactive until they verify)
            user, _ = User.objects.get_or_create(
                email=valid["email"],
                defaults={
                    "username": valid["email"],
                    "first_name": valid["first_name"],
                    "last_name": valid["last_name"],
                    "is_active": False,
                },
            )

            # check if their email is already verified
            profile, _ = UserProfile.objects.get_or_create(user=user)
            email_verified = bool(profile and profile.email_verified)

            # if not, kick off allauth’s confirmation email
            if not email_verified:
                send_email_confirmation(request, user)

            return Response(
                {
                    "detail": "Request submitted successfully.",
                    "email_verified": email_verified,
                },
                status=status.HTTP_200_OK,
            )

        # validation errors
        return Response(
            serializer.errors,
            status=status.HTTP_400_BAD_REQUEST
        )


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
    """Checks whether a user has already submitted a signup request."""

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
