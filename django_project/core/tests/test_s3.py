# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for S3 utils.
"""
import os
import boto3
import mock
from botocore.stub import Stubber
from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.test import TestCase

from core.utils.s3 import (
    zip_folder_in_s3,
    remove_s3_folder,
    create_s3_bucket,
    remove_s3_folder_by_batch,
    s3_file_exists,
    s3_compatible_env
)


class TestS3Utilities(TestCase):
    """Test S3 utilities."""

    def setUp(self):
        """Set up the test case."""
        self.bucket = 'test-bucket'
        self.prefix = 'zarr-dataset/'
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.stubber = Stubber(self.s3)
        self.stubber.activate()
        self.env_patcher = mock.patch('os.environ', {})
        self.mock_environ = self.env_patcher.start()

    def tearDown(self):
        """Tear down the test case."""
        self.stubber.deactivate()
        self.env_patcher.stop()

    def test_bucket_already_created(self):
        """Test S3 bucket already created."""
        self.assertFalse(create_s3_bucket(settings.GAP_S3_MEDIA_BUCKET_NAME))

    def test_zip_folder_in_s3(self):
        """Test zip folder in S3."""
        folder = 'test_folder'
        remove_s3_folder(default_storage, folder)
        default_storage.save(
            os.path.join(folder, 'test'), ContentFile(b"new content")
        )
        default_storage.save(
            os.path.join(folder, 'test_2'), ContentFile(b"new content")
        )
        zip_folder_in_s3(
            default_storage, folder, 'test_folder.zip'
        )
        self.assertTrue(
            default_storage.exists('test_folder.zip')
        )
        self.assertFalse(
            default_storage.exists(folder)
        )

    def test_remove_s3_folder_by_batch(self):
        """Test remove S3 folder by batch."""
        list_response = {
            'Contents': [
                {'Key': 'zarr-dataset/.zarray'},
                {'Key': 'zarr-dataset/0.0.0'},
                {'Key': 'zarr-dataset/0.0.1'},
            ],
            'IsTruncated': False
        }

        delete_request = {
            'Bucket': self.bucket,
            'Delete': {
                'Objects': [
                    {'Key': 'zarr-dataset/.zarray'},
                    {'Key': 'zarr-dataset/0.0.0'},
                    {'Key': 'zarr-dataset/0.0.1'},
                ]
            }
        }

        delete_response = {
            'Deleted': delete_request['Delete']['Objects']
        }

        self.stubber.add_response('list_objects_v2', list_response, {
            'Bucket': self.bucket,
            'Prefix': self.prefix
        })
        self.stubber.add_response(
            'delete_objects',
            delete_response,
            delete_request
        )
        result = remove_s3_folder_by_batch(self.bucket, self.prefix, self.s3)

        self.assertEqual(result['total_deleted'], 3)
        self.assertEqual(result['total_batches'], 1)
        self.stubber.assert_no_pending_responses()

    def test_s3_file_exists(self):
        """Test if S3 file exists."""
        key = 'zarr-dataset/.zarray'
        self.stubber.add_response(
            'head_object', {}, {'Bucket': self.bucket, 'Key': key}
        )

        exists = s3_file_exists(self.s3, self.bucket, key)
        self.assertTrue(exists)

        self.stubber.assert_no_pending_responses()

        # Test for a non-existing file
        self.stubber.add_client_error(
            'head_object',
            'NoSuchKey',
            {'Bucket': self.bucket, 'Key': 'non-existing-key'}
        )

        exists = s3_file_exists(self.s3, self.bucket, 'non-existing-key')
        self.assertFalse(exists)

        self.stubber.assert_no_pending_responses()

    def test_basic_functionality_https(self):
        """Test basic functionality with HTTPS endpoint."""
        with s3_compatible_env(
            'test_access', 'test_secret', 'https://s3.example.com'
        ):
            self.assertEqual(
                self.mock_environ['AWS_ACCESS_KEY_ID'], 'test_access'
            )
            self.assertEqual(
                self.mock_environ['AWS_SECRET_ACCESS_KEY'], 'test_secret'
            )
            self.assertEqual(
                self.mock_environ['AWS_S3_ENDPOINT'], 's3.example.com'
            )
            self.assertEqual(
                self.mock_environ['AWS_HTTPS'], 'YES'
            )
            self.assertEqual(
                self.mock_environ['AWS_VIRTUAL_HOSTING'], 'FALSE'
            )
            self.assertEqual(
                self.mock_environ['AWS_REGION'], 'us-east-1'
            )
        self.assertIsNone(self.mock_environ.get('AWS_S3_ENDPOINT'))

    def test_http_endpoint(self):
        """Test functionality with HTTP endpoint."""
        with s3_compatible_env(
            'test_access', 'test_secret', 'http://s3.example.com'
        ):
            self.assertEqual(
                self.mock_environ['AWS_S3_ENDPOINT'], 's3.example.com'
            )
            self.assertEqual(
                self.mock_environ['AWS_HTTPS'], 'NO'
            )

    def test_endpoint_with_trailing_slash(self):
        """Test endpoint URL processing with trailing slash."""
        with s3_compatible_env(
            'test_access', 'test_secret', 'https://s3.example.com/'
        ):
            self.assertEqual(
                self.mock_environ['AWS_S3_ENDPOINT'], 's3.example.com'
            )
