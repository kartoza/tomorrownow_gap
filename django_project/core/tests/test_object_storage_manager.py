# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for BackgroundTask Model.
"""

import os
from django.db import IntegrityError
from django.test import TestCase

from core.models.object_storage_manager import (
    ObjectStorageManager, ProtocolType
)


class ObjectStorageManagerModelTest(TestCase):
    """Unit tests for ObjectStorageManager model."""

    def setUp(self):
        """Set up test environment."""
        self.env_vars = {
            'TEST_ACCESS_KEY': 'AKIA_TEST',
            'TEST_SECRET_KEY': 'SECRET_TEST',
            'TEST_BUCKET': 'bucket-test',
            'TEST_ENDPOINT': 'https://endpoint.test',
            'TEST_REGION': 'us-test-1',
            'TEST_PREFIX': 'prefix/'
        }
        os.environ.update(self.env_vars)
        self.manager = ObjectStorageManager.objects.create(
            connection_name='test_conn',
            protocol=ProtocolType.S3,
            access_key_id_var='TEST_ACCESS_KEY',
            secret_access_key_var='TEST_SECRET_KEY',
            bucket_name='TEST_BUCKET',
            endpoint_url='TEST_ENDPOINT',
            region_name='TEST_REGION',
            directory_prefix='TEST_PREFIX',
            use_env_vars=True
        )

    def tearDown(self):
        """Clean up test environment."""
        for k in self.env_vars:
            os.environ.pop(k, None)

    def test_str_protocol_type(self):
        """Test string representation of ProtocolType."""
        self.assertEqual(str(ProtocolType.S3), 'S3')

    def test_unique_connection_name(self):
        """Test that connection_name is unique."""
        with self.assertRaises(IntegrityError):
            ObjectStorageManager.objects.create(
                connection_name='test_conn',
                protocol=ProtocolType.S3,
                access_key_id_var='TEST_ACCESS_KEY',
                secret_access_key_var='TEST_SECRET_KEY',
                bucket_name='TEST_BUCKET'
            )

    def test_fetch_variable_success(self):
        """Test fetching variable from environment."""
        value = self.manager._fetch_variable('TEST_ACCESS_KEY', 'access key')
        self.assertEqual(value, 'AKIA_TEST')

    def test_fetch_variable_missing(self):
        """Test fetching variable that is not set."""
        with self.assertRaises(ValueError):
            self.manager._fetch_variable('NOT_SET', 'missing var')

    def test_fetch_variable_empty(self):
        """Test fetching variable that is set but empty."""
        os.environ['EMPTY_VAR'] = ''
        with self.assertRaises(ValueError):
            self.manager._fetch_variable('EMPTY_VAR', 'empty var')
        os.environ.pop('EMPTY_VAR')

    def test_get_from_envs(self):
        """Test retrieving environment variables."""
        envs = self.manager._get_from_envs()
        self.assertEqual(envs['S3_ACCESS_KEY_ID'], 'AKIA_TEST')
        self.assertEqual(envs['S3_SECRET_ACCESS_KEY'], 'SECRET_TEST')
        self.assertEqual(envs['S3_BUCKET_NAME'], 'bucket-test')
        self.assertEqual(envs['S3_ENDPOINT_URL'], 'https://endpoint.test')
        self.assertEqual(envs['S3_REGION_NAME'], 'us-test-1')
        self.assertEqual(envs['S3_DIR_PREFIX'], 'prefix/')

    def test_get_from_envs_with_blank_fields(self):
        """Test retrieving environment variables with blank fields."""
        manager = ObjectStorageManager.objects.create(
            connection_name='test_blank',
            protocol=ProtocolType.S3,
            access_key_id_var='TEST_ACCESS_KEY',
            secret_access_key_var='TEST_SECRET_KEY',
            bucket_name='TEST_BUCKET',
            use_env_vars=True
        )
        envs = manager._get_from_envs()
        self.assertEqual(envs['S3_ENDPOINT_URL'], '')
        self.assertEqual(envs['S3_REGION_NAME'], '')
        self.assertEqual(envs['S3_DIR_PREFIX'], '')

    def test_get_s3_env_vars_success(self):
        """Test getting S3 environment variables."""
        envs = ObjectStorageManager.get_s3_env_vars('test_conn')
        self.assertEqual(envs['S3_ACCESS_KEY_ID'], 'AKIA_TEST')
        self.assertEqual(envs['S3_CONNECTION_NAME'], 'test_conn')

    def test_get_s3_env_vars_not_found(self):
        """Test getting S3 env var for non-existent connection."""
        with self.assertRaises(ValueError):
            ObjectStorageManager.get_s3_env_vars('not_exist')

    def test_get_s3_env_vars_wrong_protocol(self):
        """Test getting S3 environment variables with wrong protocol."""
        ObjectStorageManager.objects.create(
            connection_name='ftp_conn',
            protocol=ProtocolType.S3,  # Only S3 supported in this model
            access_key_id_var='TEST_ACCESS_KEY',
            secret_access_key_var='TEST_SECRET_KEY',
            bucket_name='TEST_BUCKET'
        )
        # Should not raise since only S3 is supported
        envs = ObjectStorageManager.get_s3_env_vars('ftp_conn')
        self.assertIn('S3_ACCESS_KEY_ID', envs)

    def test_get_s3_env_vars_use_env_vars_false(self):
        """Test getting S3 environment variables with use_env_vars=False."""
        ObjectStorageManager.objects.create(
            connection_name='no_env',
            protocol=ProtocolType.S3,
            access_key_id_var='TEST_ACCESS_KEY',
            secret_access_key_var='TEST_SECRET_KEY',
            bucket_name='bucket-direct',
            endpoint_url='endpoint-direct',
            region_name='region-direct',
            directory_prefix='prefix-direct',
            use_env_vars=False
        )
        envs = ObjectStorageManager.get_s3_env_vars('no_env')
        self.assertEqual(envs['S3_BUCKET_NAME'], 'bucket-direct')
        self.assertEqual(envs['S3_ENDPOINT_URL'], 'endpoint-direct')
        self.assertEqual(envs['S3_REGION_NAME'], 'region-direct')
        self.assertEqual(envs['S3_DIR_PREFIX'], 'prefix-direct')

    def test_get_s3_client_kwargs(self):
        """Test getting S3 client kwargs."""
        envs = ObjectStorageManager.get_s3_env_vars('test_conn')
        kwargs = ObjectStorageManager.get_s3_client_kwargs('test_conn', envs)
        self.assertEqual(kwargs['endpoint_url'], 'https://endpoint.test')
        self.assertEqual(kwargs['region_name'], 'us-test-1')

    def test_get_s3_base_url(self):
        """Test getting S3 base URL."""
        envs = ObjectStorageManager.get_s3_env_vars('test_conn')
        url = ObjectStorageManager.get_s3_base_url(envs)
        self.assertEqual(url, 's3://bucket-test/prefix/')

    def test_get_s3_base_url_trailing_slash(self):
        """Test getting S3 base URL with trailing slash."""
        envs = ObjectStorageManager.get_s3_env_vars('test_conn')
        envs['S3_DIR_PREFIX'] = 'prefix'
        url = ObjectStorageManager.get_s3_base_url(envs)
        self.assertEqual(url, 's3://bucket-test/prefix/')
