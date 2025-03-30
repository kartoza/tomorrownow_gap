# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Preferences

"""

from django.contrib.gis.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


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
    description = models.TextField(
        help_text=_("Describe your request or interest.")
    )
    submitted_at = models.DateTimeField(
        verbose_name=_('Submitted at'),
        default=timezone.now
    )

    class Meta:
        """Meta class."""

        db_table = 'sign_up_request'
        verbose_name = _('Sign up request')
        ordering = ['-submitted_at']

    def __str__(self):
        return f'{self.first_name} {self.last_name} <{self.email}>'
