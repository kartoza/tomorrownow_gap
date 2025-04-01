# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Signals
"""

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.mail import send_mail
from django.contrib.auth.models import Group
from django.conf import settings
from gap.models import SignUpRequest


@receiver(post_save, sender=SignUpRequest)
def notify_user_managers_on_signup(sender, instance, created, **kwargs):
    """Notify user managers on sign up."""
    if not created:
        return

    try:
        group = Group.objects.get(name="User Manager")
        managers = group.user_set.filter(is_active=True, email__isnull=False)

        recipient_list = [user.email for user in managers if user.email]
        if recipient_list:
            send_mail(
                subject="New Sign Up Request",
                message=(
                    f"A new sign-up request has been submitted:\n\n"
                    f"Name: {instance.first_name} {instance.last_name}\n"
                    f"Email: {instance.email}\n"
                    f'Description: {instance.description or "-"}'
                ),
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=recipient_list,
                fail_silently=False,
            )
    except Group.DoesNotExist:
        pass
