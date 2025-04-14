# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User model.

"""

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model


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
