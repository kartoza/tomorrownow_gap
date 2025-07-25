# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for S3.
"""

import io
import zipfile

import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.core.files.base import ContentFile
from storages.backends.s3boto3 import S3Boto3Storage


def zip_folder_in_s3(
        s3_storage: S3Boto3Storage, folder_path: str, zip_file_name: str
):
    """Zip folder contents into a zip file on S3."""
    zip_buffer = io.BytesIO()

    if s3_storage.exists(zip_file_name):
        s3_storage.delete(zip_file_name)

    # Create buffer zip file
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        # Get file list
        files_in_folder = s3_storage.bucket.objects.filter(
            Prefix=folder_path
        )

        for s3_file in files_in_folder:
            file_name = s3_file.key.split('/')[-1]
            if not file_name:
                continue

            # Read the file and add to zip file
            file_content = s3_file.get()['Body'].read()
            zip_file.writestr(file_name, file_content)

    # Save it to S3
    zip_buffer.seek(0)
    s3_storage.save(zip_file_name, ContentFile(zip_buffer.read()))
    remove_s3_folder(s3_storage, folder_path)


def remove_s3_folder(s3_storage: S3Boto3Storage, folder_path: str):
    """Remove folder from S3 storage."""
    if not folder_path.endswith('/'):
        folder_path += '/'

    # Get all file in the folder and remove one by one
    bucket = s3_storage.bucket
    objects_to_delete = bucket.objects.filter(Prefix=folder_path)
    for obj in objects_to_delete:
        obj.delete()


def create_s3_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region."""
    # Create bucket
    try:
        s3_client = boto3.client(
            's3',
            region_name=region,
            endpoint_url=settings.GAP_S3_ENDPOINT_URL,
            aws_access_key_id=settings.GAP_S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.GAP_S3_SECRET_ACCESS_KEY
        )
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
    except ClientError:
        return False
    return True


def remove_s3_folder_by_batch(bucket_name, prefix, s3_client):
    """Delete all objects in a folder S3."""
    paginator = s3_client.get_paginator('list_objects_v2')
    total_deleted = 0
    total_batches = 0

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        contents = page.get('Contents', [])
        objects = [{'Key': obj['Key']} for obj in contents]
        if not objects:
            continue

        # Delete in batches (max 1000 per request)
        for i in range(0, len(objects), 1000):
            batch = objects[i:i + 1000]
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': batch}
            )
            deleted = response.get('Deleted', [])
            total_deleted += len(deleted)
            total_batches += 1

    return {
        'total_deleted': total_deleted,
        'total_batches': total_batches
    }


def s3_file_exists(s3_client, bucket_name, key):
    """Check if a file exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True  # File exists
    except ClientError as e:
        # If it's a 404 error, the file doesn't exist
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # Some other error occurred (e.g., permissions issue)
            raise
