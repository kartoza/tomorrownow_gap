"""Sign-up access requests."""

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from core.serializers import SignUpRequestSerializer


class SignUpRequestView(APIView):
    """Handles POST sign-up access requests."""

    permission_classes = [AllowAny]

    def post(self, request):
        """Handle POST requests."""
        serializer = SignUpRequestSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(
                {"detail": "Request submitted successfully."},
                status=status.HTTP_200_OK
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
