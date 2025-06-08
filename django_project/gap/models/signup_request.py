# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Sign up request model.

"""

import logging
from django.contrib.gis.db import models
from django.conf import settings
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.sites.shortcuts import get_current_site
from django.core.mail import send_mail
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group


User = get_user_model()
logger = logging.getLogger(__name__)


class RequestStatus(models.TextChoices):
    """Request status choices."""

    PENDING = 'PENDING', _('Pending')
    APPROVED = 'APPROVED', _('Approved')
    REJECTED = 'REJECTED', _('Rejected')


class SignUpRequest(models.Model):
    """Model for sign up request."""

    first_name = models.CharField(
        verbose_name=_('First name'),
        max_length=100
    )
    last_name = models.CharField(
        verbose_name=_('Last name'),
        max_length=100
    )
    email = models.EmailField(
        verbose_name=_('Email'),
        unique=True
    )
    organization = models.CharField(
        verbose_name=_('Organization'),
        blank=True,
        null=True,
        max_length=200,
        help_text=_("Name of the organization you represent.")
    )
    description = models.TextField(
        help_text=_("Describe your request or interest.")
    )
    status = models.CharField(
        verbose_name=_('Status'),
        max_length=10,
        choices=RequestStatus.choices,
        default=RequestStatus.PENDING
    )
    submitted_at = models.DateTimeField(
        verbose_name=_('Submitted at'),
        default=timezone.now
    )
    approved_at = models.DateTimeField(
        verbose_name=_('Approved at'),
        default=timezone.now
    )
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        verbose_name=_('Approved by'),
        on_delete=models.SET_NULL,
        related_name='approved_requests',
        blank=True,
        null=True
    )

    class Meta:
        """Meta class."""

        db_table = 'sign_up_request'
        verbose_name = _('Sign up request')
        ordering = ['-submitted_at']

    def __str__(self):
        return f'{self.first_name} {self.last_name} <{self.email}>'


@receiver(post_save, sender=SignUpRequest)
def notify_user_managers_on_signup(sender, instance, created, **kwargs):
    """Notify user managers on sign up."""
    email_verified = False
    # check if the user's email has already been verified
    if User.objects.filter(email=instance.email).exists():
        user = User.objects.get(email=instance.email)
        if user.userprofile:
            email_verified = user.userprofile.email_verified

    if not email_verified:
        # If the email is not verified, skip sending the notification
        logger.info(
            f"Skipping notification for {instance.email} "
            "as email is not verified."
        )
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


@receiver(post_save, sender=SignUpRequest)
def send_approval_email_and_activate_user(sender, instance, created, **kwargs):
    """Activate user and send email if sign-up request is approved."""
    if created:
        return  # Skip newly created SignUpRequests

    if instance.status == RequestStatus.APPROVED and instance.email:
        User = get_user_model()

        try:
            # Activate the corresponding user
            user = User.objects.get(email=instance.email)

            email_verified = False
            # Check if the user's email has already been verified
            if user.userprofile:
                email_verified = user.userprofile.email_verified
            if not email_verified:
                # log that the email is not verified
                logger.info(
                    f"Email {instance.email} is not verified, "
                    "skipping activation and approval email."
                )
                return

            if not user.is_active:
                user.is_active = True
                user.save()

            # Send approval email
            site = get_current_site(None)  # Get the current site
            send_mail(
                subject="Your Account Signup Has Been Approved",
                message=(
                    f"Hello {instance.first_name},\n\n"
                    "Your sign-up request has been approved. "
                    "You can now log in to the platform.\n\n"
                    f"Homepage: {site.domain}\n\n"
                    "Best regards,\n"
                    "Global Access Platform Team"
                ),
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[instance.email],
                fail_silently=False,
            )
        except User.DoesNotExist:
            # Optionally log if no user was found for this email
            pass
    elif instance.status == RequestStatus.REJECTED and instance.email:
        User = get_user_model()

        try:
            # Ensure user is not active
            user = User.objects.get(email=instance.email)
            if not user.is_active:
                user.save()
            else:
                user.is_active = False
                user.save()

            # Send approval email
            send_mail(
                subject="Your Account Signup Has Been Rejected",
                message=(
                    f"Hello {instance.first_name},\n\n"
                    "Your sign-up request has been rejected. "
                    "You cannot log in to the platform.\n\n"
                    "Best regards,\n"
                    "Tomorrow Now GAP Team"
                ),
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[instance.email],
                fail_silently=False,
            )
        except User.DoesNotExist:
            # Optionally log if no user was found for this email
            pass
