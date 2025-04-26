"""Email utilities."""

from django.core.mail import send_mail
from django.conf import settings
from django.contrib.sites.models import Site
from django.contrib.auth import get_user_model


def send_verification_email(user, uid, token):
    """Send a verification email to a user."""
    # For testing purposes
    if settings.DEBUG:
        domain = "http://localhost:8000"
    else:
        domain_name = Site.objects.get_current().domain
        domain = f"https://{domain_name}"

    activation_url = (
        f"{domain}/api/auth/verify-email/?uid={uid}&token={token}"
    )
    subject = "Verify Your Email"
    message = (
        f"Hello {user.first_name},"
        f"\n\nClick to verify your account:\n{activation_url}"
    )
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [user.email])


def get_admin_emails():
    """Get a list of admin emails."""
    User = get_user_model()
    admin_emails = list(
        User.objects.filter(
            is_superuser=True
        ).exclude(
            email__isnull=True
        ).exclude(
            email__exact=''
        ).values_list('email', flat=True)
    )
    return admin_emails
