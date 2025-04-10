# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User Profile Admin

"""

from django.contrib import admin
from gap.models import UserProfile


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    """User Profile Admin."""

    list_display = ('user', 'email_verified', 'verified_at')
    search_fields = ('user__email',)
    list_filter = ('email_verified',)
