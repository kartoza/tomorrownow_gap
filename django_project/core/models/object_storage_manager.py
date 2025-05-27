# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Object Storage Manager

"""

import os
from django.db import models
from django.utils.translation import gettext_lazy as _


DEFAULT_CONNECTION_NAME = 'default'


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
