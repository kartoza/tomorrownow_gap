# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Test runner.
"""

import os
from django.conf import settings
from django.test.runner import DiscoverRunner

from core.celery import app as celery_app
from core.utils.s3 import create_s3_bucket


class CustomTestRunner(DiscoverRunner):
    """Postgres schema test runner."""

    @staticmethod
    def __disable_celery():
        """Disabling celery."""
        settings.CELERY_BROKER_URL = \
            celery_app.conf.BROKER_URL = 'filesystem:///dev/null/'
        celery_app.conf.task_always_eager = True
        data = {
            'data_folder_in': '/tmp',
            'data_folder_out': '/tmp',
            'data_folder_processed': '/tmp',
        }
        settings.BROKER_TRANSPORT_OPTIONS = \
            celery_app.conf.BROKER_TRANSPORT_OPTIONS = data

    def setup_test_environment(self, **kwargs):
        """Prepare test env."""
        CustomTestRunner.__disable_celery()
        create_s3_bucket(settings.GAP_S3_MEDIA_BUCKET_NAME)
        create_s3_bucket(
            os.environ.get("GAP_S3_PRODUCTS_BUCKET_NAME", "tngap-products")
        )
        super(CustomTestRunner, self).setup_test_environment(**kwargs)
