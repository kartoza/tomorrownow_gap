"""Email utilities."""

from django.core.mail import send_mail
from django.conf import settings
from django.contrib.sites.models import Site


def send_verification_email(user, uid, token):
    """Send a verification email to a user."""
    current_site = Site.objects.get_current()
    domain = current_site.domain
    activation_url = f"https://{domain}/verify-email/?uid={uid}&token={token}"
    subject = "Verify Your Email"
    message = (
        f"Hello {user.first_name},"
        f"\n\nClick to verify your account:\n{activation_url}"
    )
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [user.email])
