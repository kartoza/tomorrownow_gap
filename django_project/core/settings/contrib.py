# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Settings for 3rd party.
"""
from .base import *  # noqa
from .utils import absolute_path

# Extra installed apps
INSTALLED_APPS = INSTALLED_APPS + (
    'rest_framework',
    'rest_framework_gis',
    'knox',
    'webpack_loader',
    'guardian',
    'django_cleanup.apps.CleanupConfig',
    'django_celery_beat',
    'django_celery_results',
    'drf_yasg',
    'rest_framework_tracking',
    'django_admin_inline_paginator',
    'import_export',
    'import_export_celery'
)

WEBPACK_LOADER = {
    'DEFAULT': {
        'BUNDLE_DIR_NAME': 'frontend/',  # must end with slash
        'STATS_FILE': absolute_path('frontend', 'webpack-stats.prod.json'),
        'POLL_INTERVAL': 0.1,
        'TIMEOUT': None,
        'IGNORE': [r'.+\.hot-update.js', r'.+\.map'],
        'LOADER_CLASS': 'webpack_loader.loader.WebpackLoader',
    }
}
REST_FRAMEWORK = {
    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema',
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'knox.auth.TokenAuthentication',
        'rest_framework.authentication.BasicAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
    'DEFAULT_VERSIONING_CLASS': (
        'rest_framework.versioning.NamespaceVersioning'
    ),
}

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',  # default
    'guardian.backends.ObjectPermissionBackend',
)
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
CELERY_RESULT_BACKEND = 'django-db'
CELERY_RESULT_EXTENDED = True
IMPORT_EXPORT_CELERY_TASK = 'import_export.tasks.export'
IMPORT_EXPORT_CELERY_INIT_MODULE = 'GAP.celery'
IMPORT_EXPORT_CELERY_EXCLUDED_FORMATS = [
    "tsv", "json", "yaml", "html",
]

TEMPLATES[0]['OPTIONS']['context_processors'] += [
    'django.template.context_processors.request',
]

SENTRY_DSN = os.environ.get('SENTRY_DSN', '')

# Disable log API request body
DRF_TRACKING_DECODE_REQUEST_BODY = False
