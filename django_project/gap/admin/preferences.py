# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admin Preferences

"""
from django.contrib import admin

from gap.models import Preferences


@admin.register(Preferences)
class PreferencesAdmin(admin.ModelAdmin):
    """Preferences Admin."""

    fieldsets = (
        (
            None, {
                'fields': (
                    'area_of_interest',
                    'salient_area'
                )
            }
        ),
        (
            'Crop Plan', {
                'fields': (
                    'crop_plan_config',
                )
            }
        ),
        (
            'Documentation', {
                'fields': (
                    'documentation_url',
                )
            }
        ),
        (
            'Arable', {
                'fields': (
                    'arable_api_url',
                )
            }
        ),
        (
            'Tahmo', {
                'fields': (
                    'tahmo_api_url',
                )
            }
        ),
        (
            'Ingestor Config', {
                'fields': (
                    'ingestor_config',
                    'dask_threads_num',
                )
            }
        ),
        (
            'Logging', {
                'fields': (
                    'api_log_batch_size',
                )
            }
        ),
    )
