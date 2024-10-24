# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message admins
"""

from django.contrib import admin
from modeltranslation.admin import TranslationAdmin

from message.models import MessageTemplate


@admin.register(MessageTemplate)
class MessageTemplateAdmin(TranslationAdmin):
    """Admin page for MessageTemplate."""

    list_display = ('code', 'name', 'application', 'group', 'template')
    filter = ('application', 'group')
    search_fields = ('code',)

    class Media:  # noqa
        js = (
            'http://ajax.googleapis.com/ajax/libs/jquery/1.9.1/jquery.min.js',
            (
                'http://ajax.googleapis.com/'
                'ajax/libs/jqueryui/1.10.2/jquery-ui.min.js'
            ),
            'modeltranslation/js/tabbed_translation_fields.js',
        )
        css = {
            'screen': ('modeltranslation/css/tabbed_translation_fields.css',),
        }
