# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Sign up request model.

"""

from django.contrib.gis.db import models
from django.conf import settings
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


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
        blank=True,
        null=True
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
