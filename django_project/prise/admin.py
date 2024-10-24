# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Prise admins
"""
from django.contrib import admin

from prise.models import PriseMessage


@admin.register(PriseMessage)
class PriseMessageAdmin(admin.ModelAdmin):
    """Admin page for PriseMessage."""

    list_display = ('pest', 'farm_group', 'message_count')

    filter_horizontal = ('messages',)

    def message_count(self, obj: PriseMessage):
        """Message count."""
        return obj.messages.count()
