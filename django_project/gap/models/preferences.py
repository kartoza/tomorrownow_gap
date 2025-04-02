# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Preferences

"""

from datetime import datetime, tzinfo

from django.contrib.gis.db import models
from django.contrib.gis.geos import Polygon
from boto3.s3.transfer import TransferConfig

from core.models.singleton import SingletonModel
from gap.utils.dms import dms_string_to_point

sw_point = dms_string_to_point('''-27°0'0"S 21°8'0"E''')
ne_point = dms_string_to_point('''16°0'0"N 52°0'0"E''')


def area_of_interest_default():
    """Return polygon default for area of interest."""
    sw_lon, sw_lat = sw_point.x, sw_point.y
    ne_lon, ne_lat = ne_point.x, ne_point.y

    coordinates = [
        (sw_lon, sw_lat),
        (ne_lon, sw_lat),
        (ne_lon, ne_lat),
        (sw_lon, ne_lat),
        (sw_lon, sw_lat)
    ]
    return Polygon(coordinates)


def area_of_salient_default():
    """Return polygon default for salient collector."""
    coordinates = [
        (41.89, 3.98),
        (35.08, 4.87),
        (30.92, 3.57),
        (28.66, -2.48),
        (31.13, -8.62),
        (34.6, -11.74),
        (40.65, -10.68),
        (39.34, -4.73),
        (41.56, -1.64),
        (41.9, 3.98),
        (41.89, 3.98)
    ]
    return Polygon(coordinates)


def crop_plan_config_default() -> dict:
    """Return dictionary for crop plan config."""
    return {
        'lat_lon_decimal_digits': -1,
        'tz': '+03:00'  # East Africa Time
    }


def user_file_uploader_config_default() -> dict:
    """Return dictionary for user file uploader config."""
    return {
        'max_concurrency': 2,
        # upload chunk size to 500 MB
        'default_block_size': 500 * 1024 * 1024
    }


def default_dcas_config() -> dict:
    """Return dictionary for default dcas config."""
    return {
        'weekdays': [1],  # Monday=0, Sunday=6
        'override_request_date': None,
        'farm_npartitions': None,
        'grid_crop_npartitions': None,
        'farm_registries': [],
        'store_csv_to_minio': False,
        'store_csv_to_sftp': False,
    }


class Preferences(SingletonModel):
    """Preference settings specifically for gap."""

    area_of_interest = models.PolygonField(
        srid=4326, default=area_of_interest_default
    )

    crop_plan_config = models.JSONField(
        default=crop_plan_config_default,
        blank=True
    )

    # salient config
    salient_area = models.PolygonField(
        srid=4326, default=area_of_salient_default,
        help_text='Area that Salient collector will use to pull the data'
    )

    # Documentations
    documentation_url = models.URLField(
        default='https://kartoza.github.io/tomorrownow_gap/',
        null=True,
        blank=True
    )

    # Arable
    arable_api_url = models.CharField(
        max_length=256,
        default='https://api.arable.cloud/api/v2',
        null=True,
        blank=True
    )

    # Tahmo
    tahmo_api_url = models.CharField(
        max_length=256,
        default='https://datahub.tahmo.org',
        null=True,
        blank=True
    )

    # dask config
    dask_threads_num = models.IntegerField(
        default=2,
        help_text=(
            'Number of threads for dask parallel computation, '
            'higher number will use more memory.'
        )
    )

    dask_threads_num_api = models.IntegerField(
        default=2,
        help_text=(
            'Number of threads for dask parallel computation in API, '
            'higher number will use more memory.'
        )
    )

    # ingestor config
    ingestor_config = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text=(
            'Dict of ProviderName and AdditionalConfig; '
            'AdditionalConfig will be passed to the Ingestor Session.'
        )
    )

    # api log batch size
    api_log_batch_size = models.IntegerField(
        default=500,
        help_text='Number of API Request logs to be saved in a batch.'
    )

    # api use x-accel-redirect
    api_use_x_accel_redirect = models.BooleanField(
        default=True,
        help_text=(
            'When set to True, Django will send X-Accel-Redirect header '
            'to the NGINX to offload the download process to NGINX.'
        )
    )

    # api log batch size
    api_use_parquet = models.BooleanField(
        default=False,
        help_text=(
            'When set to True, API will use parquet reader '
            'instead of EAV.'
        )
    )

    # UserFile Uploader s3 config
    user_file_uploader_config = models.JSONField(
        default=user_file_uploader_config_default,
        blank=True,
        null=True,
        help_text='Config for fsspec uploader to s3 for UserFile.'
    )

    # DCAS Configuration
    dcas_config = models.JSONField(
        default=default_dcas_config,
        blank=True,
        null=True,
        help_text='Config for DCAS module.'
    )

    # duckdb configuration
    duckdb_threads_num = models.IntegerField(
        default=2,
        help_text=(
            'Number of threads for duckdb computation, '
            'higher number will use more memory and cpu.'
        )
    )

    class Meta:  # noqa: D106
        verbose_name_plural = "preferences"

    def __str__(self):
        return 'Preferences'

    @staticmethod
    def lat_lon_decimal_digits() -> int:
        """Return decimal digits for latitude and longitude."""
        crop_plan_conf = Preferences.load().crop_plan_config
        return crop_plan_conf.get(
            'lat_lon_decimal_digits',
            crop_plan_config_default()['lat_lon_decimal_digits']
        )

    @staticmethod
    def east_africa_timezone() -> tzinfo:
        """Return east african time zone."""
        crop_plan_conf = Preferences.load().crop_plan_config
        timezone = crop_plan_conf.get(
            'tz',
            crop_plan_config_default()['tz']
        )
        return datetime.strptime(timezone, "%z").tzinfo

    @staticmethod
    def user_file_s3_transfer_config() -> TransferConfig:
        """Get S3 transfer config for GAP Products."""
        conf = Preferences.load().user_file_uploader_config
        return TransferConfig(
            multipart_chunksize=(
                conf.get('default_block_size', 500 * 1024 * 1024)
            ),
            use_threads=True,
            max_concurrency=(
                conf.get('max_concurrency', 2)
            )
        )
