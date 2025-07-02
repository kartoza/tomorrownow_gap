# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Object Storage Manager

"""

import os
import boto3
from django.db import models
from django.utils.translation import gettext_lazy as _
from django.http import StreamingHttpResponse
from django.utils import timezone

from core.models.background_task import TaskStatus


DEFAULT_CONNECTION_NAME = 'default'
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MB


class ProtocolType(models.TextChoices):
    """Protocol type choices."""

    S3 = 'S3', _('S3')


class ObjectStorageManager(models.Model):
    """Class that represents managing access to Object Storage."""

    connection_name = models.CharField(
        max_length=128,
        unique=True,
        verbose_name=_('Connection Name'),
        help_text=_('Unique name for the connection to the object storage.')
    )
    protocol = models.CharField(
        max_length=255,
        choices=ProtocolType.choices,
        default=ProtocolType.S3
    )
    access_key_id_var = models.CharField(
        max_length=255,
        verbose_name=_('Access Key ID Variable')
    )
    secret_access_key_var = models.CharField(
        max_length=255,
        verbose_name=_('Secret Access Key Variable')
    )
    bucket_name = models.CharField(
        max_length=255,
        verbose_name=_('Bucket Name')
    )
    endpoint_url = models.CharField(
        max_length=255,
        verbose_name=_('Endpoint URL'),
        blank=True,
        null=True
    )
    region_name = models.CharField(
        max_length=255,
        verbose_name=_('Region Name'),
        blank=True,
        null=True
    )
    directory_prefix = models.CharField(
        max_length=255,
        verbose_name=_('Directory Prefix'),
        blank=True,
        null=True
    )
    use_env_vars = models.BooleanField(
        default=True,
        verbose_name=_('Use Environment Variables'),
        help_text=_(
            'If checked, the bucket_name, endpoint_url, region_name, '
            'directory_prefix will be '
            'retrieved from environment variables.'
        )
    )
    created_on = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_('Created On')
    )

    def _fetch_variable(
        self, variable_name, variable_desc, allow_empty=False
    ):
        """Fetch variable from environment variables."""
        value = os.environ.get(variable_name, None)
        if value is None:
            raise ValueError(
                f'{self.protocol} {variable_desc} variable '
                f'"{variable_name}" is not set in '
                'environment variables.'
            )
        if not allow_empty and not value:
            raise ValueError(
                f'{self.protocol} {variable_desc} variable '
                f'"{variable_name}" is empty.'
            )
        return value

    def _get_from_envs(self):
        """Retrieve keys from environment variables."""
        access_key_id = self._fetch_variable(
            self.access_key_id_var,
            'access key ID'
        )
        secret_access_key = self._fetch_variable(
            self.secret_access_key_var,
            'secret access key'
        )
        result = {
            f'{self.protocol}_ACCESS_KEY_ID': access_key_id,
            f'{self.protocol}_SECRET_ACCESS_KEY': secret_access_key
        }
        if not self.use_env_vars:
            return result

        # get bucket name
        result[f'{self.protocol}_BUCKET_NAME'] = self._fetch_variable(
            self.bucket_name,
            'bucket name'
        )

        # get endpoint url
        if self.endpoint_url:
            result[f'{self.protocol}_ENDPOINT_URL'] = self._fetch_variable(
                self.endpoint_url,
                'endpoint URL',
                allow_empty=True
            )
        else:
            result[f'{self.protocol}_ENDPOINT_URL'] = ''

        # get region name
        if self.region_name:
            result[f'{self.protocol}_REGION_NAME'] = self._fetch_variable(
                self.region_name,
                'region name',
                allow_empty=True
            )
        else:
            result[f'{self.protocol}_REGION_NAME'] = ''

        # get directory prefix
        if self.directory_prefix:
            result[f'{self.protocol}_DIR_PREFIX'] = self._fetch_variable(
                self.directory_prefix,
                'directory prefix',
                allow_empty=True
            )
        else:
            result[f'{self.protocol}_DIR_PREFIX'] = ''

        return result

    @classmethod
    def get_s3_env_vars(cls, connection_name = None) -> dict:
        """Get S3 environment variables for Object Storage Manager.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        connection_name = connection_name or DEFAULT_CONNECTION_NAME
        try:
            manager = cls.objects.get(connection_name=connection_name)
        except cls.DoesNotExist:
            raise ValueError(
                'ObjectStorageManager with connection name '
                f'"{connection_name}" does not exist.'
            )
        if manager.protocol != ProtocolType.S3:
            raise ValueError(
                'Protocol for ObjectStorageManager '
                f'"{connection_name}" is not S3.'
            )

        s3_dicts = manager._get_from_envs()
        if not manager.use_env_vars:
            # if not  using env vars, use bucket name etc from the manager
            s3_dicts.update({
                'S3_BUCKET_NAME': manager.bucket_name,
                'S3_ENDPOINT_URL': manager.endpoint_url or '',
                'S3_REGION_NAME': manager.region_name or '',
                'S3_DIR_PREFIX': manager.directory_prefix or ''
            })
        # add connection name
        s3_dicts['S3_CONNECTION_NAME'] = connection_name
        return s3_dicts

    @classmethod
    def get_s3_client_kwargs(
        cls, connection_name = None, s3: dict = None
    ) -> dict:
        """Get S3 client kwargs for Object Storage Manager.

        :return: Dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        if s3 is None:
            s3 = cls.get_s3_env_vars(connection_name)
        client_kwargs = {}
        if s3.get('S3_ENDPOINT_URL'):
            client_kwargs['endpoint_url'] = s3['S3_ENDPOINT_URL']
        if s3.get('S3_REGION_NAME'):
            client_kwargs['region_name'] = s3['S3_REGION_NAME']
        return client_kwargs

    @classmethod
    def get_s3_base_url(cls, s3: dict) -> str:
        """Generate S3 base URL.

        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :return: Base URL with S3 and bucket name
        :rtype: str
        """
        prefix = s3['S3_DIR_PREFIX']
        bucket_name = s3['S3_BUCKET_NAME']
        s3_url = f's3://{bucket_name}/{prefix}'
        if not s3_url.endswith('/'):
            s3_url += '/'
        return s3_url

    @classmethod
    def get_s3_client(cls, s3: dict = None, connection_name: str = None):
        """Get S3 client for Object Storage Manager.

        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :param connection_name: Connection name for Object Storage Manager
        :type connection_name: str
        :return: Boto3 S3 client
        :rtype: boto3.client
        """
        if s3 is None:
            s3 = cls.get_s3_env_vars(connection_name)

        s3_client_kwargs = cls.get_s3_client_kwargs(connection_name, s3)
        s3_client_kwargs['aws_access_key_id'] = s3['S3_ACCESS_KEY_ID']
        s3_client_kwargs['aws_secret_access_key'] = s3['S3_SECRET_ACCESS_KEY']
        return boto3.client("s3", **s3_client_kwargs)

    @classmethod
    def upload_file_to_s3(
        cls, file_path: str, transfer_config: dict,
        remote_file_path: str, content_type: str,
        s3: dict = None, connection_name: str = None
    ) -> str:
        """Upload file to S3 storage.

        :param file_path: Path to the file to upload
        :type file_path: str
        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :param connection_name: Connection name for Object Storage Manager
        :type connection_name: str
        :return: URL of the uploaded file
        :rtype: str
        """
        if s3 is None:
            s3 = cls.get_s3_env_vars(connection_name)
        s3_client = cls.get_s3_client(s3, connection_name)

        output_url = s3["S3_DIR_PREFIX"]
        if not output_url.endswith('/'):
            output_url += '/'
        output_url += remote_file_path

        # Upload file
        s3_client.upload_file(
            Filename=file_path,
            Bucket=s3["S3_BUCKET_NAME"],
            Key=output_url,
            ExtraArgs={'ContentType': content_type},
            Config=transfer_config
        )

        # Return file URL
        return output_url

    @classmethod
    def download_file_from_s3(
        cls, remote_file_path: str, s3: dict = None,
        connection_name: str = None, chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> StreamingHttpResponse:
        """Download file from S3 storage.

        :param remote_file_path: Path to the file in S3
        :type remote_file_path: str
        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :param connection_name: Connection name for Object Storage Manager
        :type connection_name: str
        :return: Streaming HTTP response with file content
        :rtype: StreamingHttpResponse
        """
        if s3 is None:
            s3 = cls.get_s3_env_vars(connection_name)
        s3_client = cls.get_s3_client(s3, connection_name)

        s3_object = s3_client.get_object(
            Bucket=s3["S3_BUCKET_NAME"],
            Key=remote_file_path
        )
        body = s3_object['Body']

        # Generator that reads the body in chunks
        def stream_generator(chunk_size):
            while True:
                data = body.read(chunk_size)
                if not data:
                    break
                yield data

        response = StreamingHttpResponse(
            stream_generator(chunk_size),
            content_type=s3_object['ContentType']
        )
        response['Content-Disposition'] = (
            f'attachment; filename="{remote_file_path.split("/")[-1]}"'
        )
        return response


class DeletionLog(models.Model):
    """Model to log deletions of files in Object Storage."""

    object_storage_manager = models.ForeignKey(
        ObjectStorageManager,
        on_delete=models.CASCADE,
        related_name='deletion_logs'
    )
    path = models.TextField(
        help_text=_('Path of the file/folder that was deleted.')
    )
    is_directory = models.BooleanField(
        default=False,
        verbose_name=_('Is Directory'),
        help_text=_('Indicates if the deletion was for a directory.')
    )
    deleted_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_('Deleted At')
    )
    status = models.CharField(
        max_length=255,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING
    )
    started_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name=_('Started At')
    )
    finished_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name=_('Finished At')
    )
    errors = models.TextField(
        null=True,
        blank=True,
        verbose_name=_('Errors'),
        help_text=_('Any errors that occurred during deletion.')
    )
    stats = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        verbose_name=_('Deletion Stats'),
        help_text=_('Statistics about the deletion process.')
    )

    def __str__(self):
        return f'Deletion Log: {self.file_path} at {self.deleted_at}'

    def clean(self):
        """Validate the deletion log."""
        super().clean()
        if not self.object_storage_manager:
            raise ValueError(
                'Object Storage Manager must be specified for Deletion Log.'
            )
        if not self.path:
            raise ValueError('Path must be specified for Deletion Log.')
        
        # Ensure path ends with a slash for folder deletion
        if self.is_directory and not self.path.endswith('/'):
            self.path += '/'

    def run(self):
        """Run the deletion process."""
        from core.utils.s3 import remove_s3_folder_by_batch
        self.status = TaskStatus.RUNNING
        self.started_at = timezone.now()
        self.save()

        try:
            # s3_client = self.object_storage_manager.get_s3_client(
            #     connection_name=self.object_storage_manager.connection_name
            # )
            # self.stats = remove_s3_folder_by_batch(
            #     self.object_storage_manager.bucket_name,
            #     self.path,
            #     s3_client
            # )

            self.status = TaskStatus.COMPLETED
        except Exception as e:
            self.status = TaskStatus.STOPPED
            self.errors = str(e)

        self.finished_at = timezone.now()
        self.save()
