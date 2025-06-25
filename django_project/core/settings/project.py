# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Project level settings.
"""
import os  # noqa

from boto3.s3.transfer import TransferConfig

from .contrib import *  # noqa
from .utils import absolute_path

ALLOWED_HOSTS = ['*']
ADMINS = ()
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.environ['DATABASE_NAME'],
        'USER': os.environ['DATABASE_USERNAME'],
        'PASSWORD': os.environ['DATABASE_PASSWORD'],
        'HOST': os.environ['DATABASE_HOST'],
        'PORT': 5432,
        'TEST_NAME': 'unittests',
    }
}

# Set debug to false for production
DEBUG = TEMPLATE_DEBUG = False

# Extra installed apps
PROJECT_APPS = (
    'core',
    'frontend',
    'gap',
    'gap_api',
    'spw',
    'message',
    'prise',
    'dcas',
    'permission'
)
INSTALLED_APPS = INSTALLED_APPS + PROJECT_APPS

TEMPLATES[0]['DIRS'] += [
    absolute_path('frontend', 'templates'),
]

MB = 1024 ** 2
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=512 * MB,
    use_threads=True,
    max_concurrency=10
)
S3_PRODUCTS_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=300 * MB,
    use_threads=True,
    max_concurrency=2
)
GAP_S3_ACCESS_KEY_ID = os.environ.get("GAP_S3_ACCESS_KEY_ID")
GAP_S3_SECRET_ACCESS_KEY = os.environ.get("GAP_S3_SECRET_ACCESS_KEY")
GAP_S3_MEDIA_BUCKET_NAME = os.environ.get("GAP_S3_MEDIA_BUCKET_NAME")
GAP_S3_ENDPOINT_URL = os.environ.get("GAP_S3_ENDPOINT_URL")
STORAGES = {
    "default": {
        "BACKEND": "storages.backends.s3.S3Storage",
        "OPTIONS": {
            "access_key": GAP_S3_ACCESS_KEY_ID,
            "secret_key": GAP_S3_SECRET_ACCESS_KEY,
            "bucket_name": GAP_S3_MEDIA_BUCKET_NAME,
            "file_overwrite": False,
            "max_memory_size": 300 * MB,  # 300MB
            "transfer_config": S3_TRANSFER_CONFIG,
            "endpoint_url": GAP_S3_ENDPOINT_URL
        },
    },
    "staticfiles": {
        "BACKEND": (
            "django.contrib.staticfiles.storage.ManifestStaticFilesStorage"
        )
    },
    "gap_products": {
        "BACKEND": "storages.backends.s3.S3Storage",
        "OPTIONS": {
            "access_key": GAP_S3_ACCESS_KEY_ID,
            "secret_key": GAP_S3_SECRET_ACCESS_KEY,
            "bucket_name": os.environ.get("GAP_S3_PRODUCTS_BUCKET_NAME"),
            "file_overwrite": False,
            "transfer_config": S3_TRANSFER_CONFIG,
            "endpoint_url": GAP_S3_ENDPOINT_URL
        },
    }
}

STORAGE_DIR_PREFIX = os.environ.get("GAP_S3_MEDIA_DIR_PREFIX", "media")
if STORAGE_DIR_PREFIX and not STORAGE_DIR_PREFIX.endswith("/"):
    STORAGE_DIR_PREFIX = f"{STORAGE_DIR_PREFIX}/"

GAP_PRODUCTS_DIR_PREFIX = os.environ.get(
    "GAP_S3_PRODUCTS_DIR_PREFIX",
    "staging"
)
if GAP_PRODUCTS_DIR_PREFIX and not GAP_PRODUCTS_DIR_PREFIX.endswith("/"):
    GAP_PRODUCTS_DIR_PREFIX = f"{GAP_PRODUCTS_DIR_PREFIX}/"

DATA_UPLOAD_MAX_NUMBER_FIELDS = 1500

# Required for import_export_celery tasks
MIDDLEWARE_CELERY = (
    'author.middlewares.AuthorDefaultBackendMiddleware',
)

MIDDLEWARE = MIDDLEWARE + MIDDLEWARE_CELERY

# SFTP settings
SFTP_HOST = os.getenv("SFTP_HOST", "127.0.0.1")  # Default: localhost
SFTP_PORT = int(os.getenv("SFTP_PORT", "2222"))  # Default: 2222
SFTP_USERNAME = os.getenv("SFTP_USERNAME", "user")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD", "password")
SFTP_REMOTE_PATH = os.getenv(
    "SFTP_REMOTE_PATH", "upload"
)

# Swagger settings
SWAGGER_SETTINGS = {
    'LOGIN_URL': '/admin/login/',
    'LOGOUT_URL': '/admin/logout/'
}

# Celery Data Request QUEUE
CELERY_DATA_REQUEST_QUEUE = os.getenv(
    "CELERY_DATA_REQUEST_QUEUE", "data_request_queue"
)

# Tell Django to look for Knox’s migrations in our own app directory
# (so we can version-control any tweaks e.g. adding APIKeyMetadata—and avoid
# permission errors when running makemigrations against knox site-packages)
MIGRATION_MODULES = {
    "knox": "knox_local_migrations",
}

# Explicitly point Knox at its AuthToken model so our local migrations
# reference the correct token table
KNOX_TOKEN_MODEL = "knox.AuthToken"
