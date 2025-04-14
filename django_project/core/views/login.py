"""Login and Logout views for the application."""

from django.contrib.auth import (
    authenticate, login, logout,
)
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import (
    AllowAny, IsAuthenticated
)


class LoginView(APIView):
    """Login a user."""

    permission_classes = [AllowAny]

    def post(self, request):
        """Handle POST requests for login."""
        username = request.data.get("username")
        password = request.data.get("password")

        user = authenticate(request, username=username, password=password)
        if user is not None:
            if user.is_active:
                login(request, user)
                return Response(
                    {
                        "detail": "Login successful",
                        "redirect_url": "/api/v1/docs/"
                    },
                    status=status.HTTP_200_OK
                )
            return Response(
                {"detail": "User account is disabled."},
                status=status.HTTP_403_FORBIDDEN
            )
        return Response(
            {"detail": "Invalid credentials."},
            status=status.HTTP_401_UNAUTHORIZED
        )


class LogoutView(APIView):
    """Logout a user."""

    permission_classes = [IsAuthenticated]

    def post(self, request):
        """Handle POST requests for logout."""
        logout(request)
        return Response({"detail": "Logged out successfully."})
