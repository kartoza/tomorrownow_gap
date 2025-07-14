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
    'import_export_celery',

    'allauth',
    'allauth.account',
    'allauth.socialaccount',
    'allauth.socialaccount.providers.google',
    'allauth.socialaccount.providers.github',
    'allauth.socialaccount.providers.apple',
    'rest_framework.authtoken',
    # REST helpers
    'dj_rest_auth',
    'dj_rest_auth.registration',
)

# Extra middleware
MIDDLEWARE = MIDDLEWARE + (
    "allauth.account.middleware.AccountMiddleware",
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
    'allauth.account.auth_backends.AuthenticationBackend',
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

# Auth
ACCOUNT_AUTHENTICATION_METHOD = 'email'
ACCOUNT_AUTHENTICATED_LOGIN_REDIRECTS = False
ACCOUNT_UNIQUE_EMAIL = True
ACCOUNT_EMAIL_REQUIRED = True
ACCOUNT_EMAIL_VERIFICATION = 'mandatory'
ACCOUNT_CONFIRM_EMAIL_ON_GET = True
ACCOUNT_AUTHENTICATED_REMEMBER = True
ACCOUNT_USERNAME_REQUIRED = False
ACCOUNT_EMAIL_CONFIRMATION_EXPIRE_DAYS = 3
SOCIALACCOUNT_LOGIN_ON_GET = True
SOCIALACCOUNT_EMAIL_VERIFICATION = True
SOCIALACCOUNT_AUTO_SIGNUP = True
SOCIALACCOUNT_ADAPTER = 'frontend.adapters.SocialSignupAdapter'
ACCOUNT_ADAPTER = "frontend.adapters.InactiveRedirectAccountAdapter"

# For users who are already logged in when confirming
ACCOUNT_EMAIL_CONFIRMATION_AUTHENTICATED_REDIRECT_URL = '/'
# For users who are not logged in when confirming
ACCOUNT_EMAIL_CONFIRMATION_ANONYMOUS_REDIRECT_URL = '/signin/?confirmed=true'

SOCIALACCOUNT_PROVIDERS = {
    "google": {
        "SCOPE": [
            "profile",
            "email"
        ],
        'EMAIL_AUTHENTICATION': True,
        'OAUTH_PKCE_ENABLED': True,
        "AUTH_PARAMS": {
            "access_type": "online"
        }
    },
    'github': {
        'SCOPE': [
            'read:user',
            'user:email',
        ],
        'EMAIL_AUTHENTICATION': True,
        'OAUTH_PKCE_ENABLED': True,
    }
}
