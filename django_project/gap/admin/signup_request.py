# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admin Sign Up Request

"""

from django.contrib import admin
from gap.models import SignUpRequest


@admin.register(SignUpRequest)
class SignUpRequestAdmin(admin.ModelAdmin):
    """Sign Up Request Admin."""

    list_display = ('first_name', 'last_name', 'email', 'submitted_at')
    search_fields = ('first_name', 'last_name', 'email')
    list_filter = ('submitted_at',)
    ordering = ('-submitted_at',)
