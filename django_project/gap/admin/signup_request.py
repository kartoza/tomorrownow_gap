# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admin Sign Up Request

"""

from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.utils.translation import gettext_lazy as _
from gap.models import SignUpRequest


User = get_user_model()


class ApprovedByManagerFilter(admin.SimpleListFilter):
    """Filter approved_by by users in 'Manager' group."""

    title = _('Approved by (Managers only)')
    parameter_name = 'approved_by'

    def lookups(self, request, model_admin):
        """Return a list of tuples."""
        try:
            managers = User.objects.filter(groups__name='User Manager')
            return [(
                user.id, user.get_full_name() or user.username
            ) for user in managers]
        except Group.DoesNotExist:
            return []

    def queryset(self, request, queryset):
        """Return the filtered queryset."""
        if self.value():
            return queryset.filter(approved_by__id=self.value())
        return queryset


@admin.register(SignUpRequest)
class SignUpRequestAdmin(admin.ModelAdmin):
    """Sign Up Request Admin."""

    list_display = (
        'first_name', 'last_name',
        'email', 'status',
        'submitted_at', 'approved_by', 'approved_at'
    )
    search_fields = ('first_name', 'last_name', 'email')
    list_filter = ('status', 'submitted_at', ApprovedByManagerFilter)
    ordering = ('-submitted_at',)
    readonly_fields = ('submitted_at', 'approved_at')

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        """Set approved_by field to staff users only."""
        if db_field.name == 'approved_by':
            kwargs["queryset"] = User.objects.filter(is_staff=True)
        return super().formfield_for_foreignkey(db_field, request, **kwargs)
