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
                    'duckdb_threads_num'
                )
            }
        ),
        (
            'API Config', {
                'fields': (
                    'dask_threads_num_api',
                    'api_log_batch_size',
                    'api_use_x_accel_redirect',
                    'api_use_parquet',
                    'user_file_uploader_config'
                )
            }
        ),
        (
            'DCAS Config', {
                'fields': (
                    'dcas_config',
                )
            }
        ),
        (
            'User Interface', {
                'fields': (
                    'google_analytics_id',
                    "social_auth_providers",
                )
            }
        ),
        (
            'Job Execution', {
                'fields': (
                    'job_executor_config',
                )
            }
        )
    )
