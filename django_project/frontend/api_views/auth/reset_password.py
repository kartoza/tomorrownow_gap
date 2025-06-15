"""Reset Password API Views."""

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.contrib.auth import get_user_model
from django.utils.http import (
    urlsafe_base64_decode,
    urlsafe_base64_encode
)
from django.contrib.auth.tokens import default_token_generator
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny


class ForgotPasswordView(APIView):
    """View to handle password reset requests."""

    permission_classes = [AllowAny]

    def post(self, request, *args, **kwargs):
        """Handle password reset request."""
        email = request.data.get('email')

        users = get_user_model().objects.filter(email=email)
        if not users.exists():
            return Response(
                {'message': 'Password reset link sent to your email'},
                status=status.HTTP_200_OK
            )
        user = users.first()

        token = default_token_generator.make_token(user)
        uid = urlsafe_base64_encode(str(user.pk).encode())

        relative = f"/signin?uid={uid}&token={token}"
        reset_password_link = request.build_absolute_uri(relative)

        # Send the password reset email
        subject = render_to_string(
            'account/email/email_set_password_subject.txt',
            {'user': user}
        ).strip()
        html_message = render_to_string(
            'account/email/email_set_password_message.txt', {
                'user': user,
                'reset_password_url': reset_password_link,
                'django_backend_url': '/',
            })

        email_message = EmailMultiAlternatives(
            subject=subject,
            body="Please reset your password using the link below.",
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[email]
        )
        email_message.attach_alternative(html_message, "text/html")
        email_message.send()

        return Response(
            {'message': 'Password reset link sent to your email.'},
            status=status.HTTP_200_OK
        )


class ResetPasswordConfirmView(APIView):
    """View to handle password reset confirmation."""

    permission_classes = [AllowAny]

    def post(self, request, uidb64, token, *args, **kwargs):
        """Reset password confirmation view."""
        try:
            uid = urlsafe_base64_decode(uidb64).decode()
            user = get_user_model().objects.get(pk=uid)
        except (
            TypeError, ValueError,
            OverflowError,
            get_user_model().DoesNotExist
        ):
            return Response(
                {'error': 'Invalid reset link'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if default_token_generator.check_token(user, token):
            new_password = request.data.get('new_password')
            user.set_password(new_password)
            user.save()
            return Response(
                {'message': 'Password has been successfully reset.'},
                status=status.HTTP_200_OK
            )

        return Response(
            {'error': 'Invalid reset link'},
            status=status.HTTP_400_BAD_REQUEST
        )
