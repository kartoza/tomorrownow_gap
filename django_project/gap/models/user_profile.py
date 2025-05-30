# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User model.

"""

import logging
from django.utils import timezone
from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model
from allauth.account.signals import user_signed_up


logger = logging.getLogger(__name__)


class UserProfile(models.Model):
    """User profile model."""

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE
    )
    email_verified = models.BooleanField(default=False)
    verified_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        """Meta class."""

        db_table = 'userprofile'
        verbose_name = "User Profile"
        verbose_name_plural = "User Profiles"
        ordering = ['user']


User = get_user_model()


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    """Create user profile when a new user is created."""
    if created and not hasattr(instance, 'userprofile'):
        UserProfile.objects.create(user=instance)


@receiver(user_signed_up, sender=User)
def mark_social_email_verified_on_signup(request, user, **kwargs):
    try:
        is_verified = user.emailaddress_set.filter(
            email=user.email, verified=True
        ).exists()
        if is_verified:
            profile, _ = UserProfile.objects.get_or_create(user=user)
            if not profile.email_verified:
                profile.email_verified = True
                profile.verified_at = timezone.now()
                profile.save()
    except Exception as e:
        logger.warning(f"Could not set verified flag at signup: {e}")
