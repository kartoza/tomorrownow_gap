# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models
from datetime import timedelta
from django.utils import timezone

from core.models.common import Definition
from gap.models.common import Provider


# Default days to keep Zarr files marked as deleted
DEFAULT_CUTOFF_DATASOURCEFILE_DAYS = 1


class CastType:
    """Cast type."""

    HISTORICAL = 'historical'
    FORECAST = 'forecast'


class DatasetType(Definition):
    """Dataset type."""

    type = models.CharField(
        choices=(
            (CastType.HISTORICAL, CastType.HISTORICAL),
            (CastType.FORECAST, CastType.FORECAST),
        ),
        max_length=512
    )
    variable_name = models.CharField(
        max_length=512
    )


class DatasetStore:
    """Dataset Storing Type."""

    TABLE = 'TABLE'
    NETCDF = 'NETCDF'
    ZARR = 'ZARR'
    EXT_API = 'EXT_API'
    ZIP_FILE = 'ZIP_FILE'
    PARQUET = 'PARQUET'
    DUCKDB = 'DUCKDB'

    @classmethod
    def to_ext(cls, store_type: str) -> str:
        """Get file extension of a store type."""
        if store_type == DatasetStore.NETCDF:
            return '.nc'
        elif store_type == DatasetStore.ZARR:
            return '.zarr'
        elif store_type == DatasetStore.ZIP_FILE:
            return '.zip'
        elif store_type == DatasetStore.PARQUET:
            # for partitioned parquet, we don't use file ext
            return ''
        elif store_type == DatasetStore.DUCKDB:
            return '.duckdb'
        else:
            raise NotImplementedError(
                f'{store_type} does not have store implementation!'
            )


class DatasetTimeStep:
    """Dataset Time Step."""

    QUARTER_HOURLY = 'QUARTER_HOURLY'
    HOURLY = 'HOURLY'
    DAILY = 'DAILY'
    OTHER = 'OTHER'

    @classmethod
    def to_freq(cls, time_step: str) -> str:
        """Convert time_step to pandas frequency.

        :param time_step: One of DatasetTimeStep
        :type time_step: str
        :return: frequency
        :rtype: str
        """
        if time_step == DatasetTimeStep.DAILY:
            return 'D'
        elif time_step == DatasetTimeStep.HOURLY:
            return 'h'
        elif time_step == DatasetTimeStep.QUARTER_HOURLY:
            return '15min'
        else:
            raise ValueError(f'Unsupported time_step {time_step}')


class DatasetObservationType:
    """Observation type of data source."""

    GROUND_OBSERVATION = 'GROUND_OBSERVATION'
    UPPER_AIR_OBSERVATION = 'UPPER_AIR_OBSERVATION'
    NOT_SPECIFIED = 'NOT_SPECIFIED'


class Dataset(Definition):
    """Model representing dataset of measurement collection."""

    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    type = models.ForeignKey(
        DatasetType, on_delete=models.CASCADE
    )
    time_step = models.CharField(
        choices=(
            (DatasetTimeStep.DAILY, DatasetTimeStep.DAILY),
            (DatasetTimeStep.HOURLY, DatasetTimeStep.HOURLY),
            (DatasetTimeStep.QUARTER_HOURLY, DatasetTimeStep.QUARTER_HOURLY),
            (DatasetTimeStep.OTHER, DatasetTimeStep.OTHER),
        ),
        max_length=512
    )
    store_type = models.CharField(
        choices=(
            (DatasetStore.TABLE, DatasetStore.TABLE),
            (DatasetStore.NETCDF, DatasetStore.NETCDF),
            (DatasetStore.ZARR, DatasetStore.ZARR),
            (DatasetStore.EXT_API, DatasetStore.EXT_API),
            (DatasetStore.PARQUET, DatasetStore.PARQUET),
        ),
        max_length=512
    )
    is_internal_use = models.BooleanField(
        default=False,
        help_text=(
            'Indicates whether this dataset is internal use, '
            'not exposed through API.'
        ),
    )
    observation_type = models.CharField(
        choices=(
            (
                DatasetObservationType.GROUND_OBSERVATION,
                DatasetObservationType.GROUND_OBSERVATION
            ),
            (
                DatasetObservationType.UPPER_AIR_OBSERVATION,
                DatasetObservationType.UPPER_AIR_OBSERVATION
            ),
            (
                DatasetObservationType.NOT_SPECIFIED,
                DatasetObservationType.NOT_SPECIFIED
            ),
        ),
        max_length=512,
        default=DatasetObservationType.NOT_SPECIFIED
    )


class DataSourceFile(models.Model):
    """Model representing a datasource file that is stored in S3 Storage."""

    name = models.CharField(
        max_length=512,
        help_text="Filename with its path in the object storage (S3)"
    )
    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE
    )
    start_date_time = models.DateTimeField()
    end_date_time = models.DateTimeField()
    created_on = models.DateTimeField()
    format = models.CharField(
        choices=(
            (DatasetStore.NETCDF, DatasetStore.NETCDF),
            (DatasetStore.ZARR, DatasetStore.ZARR),
            (DatasetStore.ZIP_FILE, DatasetStore.ZIP_FILE),
            (DatasetStore.PARQUET, DatasetStore.PARQUET),
            (DatasetStore.DUCKDB, DatasetStore.DUCKDB),
        ),
        max_length=512
    )
    is_latest = models.BooleanField(default=False)
    metadata = models.JSONField(blank=True, default=dict, null=True)
    deleted_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="If the file is deleted, this field will be set."
    )

    def __str__(self):
        return f'{self.name} - {self.id}'

    def should_delete(self) -> bool:
        """Check if the file should be deleted based on retention policy."""
        if self.deleted_at is None:
            return False

        # If no retention config is found, use the default days
        days_to_keep = DEFAULT_CUTOFF_DATASOURCEFILE_DAYS
        # find the retention config for this dataset
        retention_config = DataSourceFileRententionConfig.objects.filter(
            dataset=self.dataset
        ).first()
        if retention_config:
            days_to_keep = retention_config.days_to_keep

        # Check if the file is marked for deletion and
        # if it has passed the retention period
        cutoff_date = self.deleted_at + timedelta(days=days_to_keep)
        return timezone.now() >= cutoff_date


class DataSourceFileCache(models.Model):
    """Model representing cache for DataSourceFile."""

    source_file = models.ForeignKey(
        DataSourceFile, on_delete=models.CASCADE
    )
    hostname = models.CharField(
        max_length=512
    )
    created_on = models.DateTimeField()
    expired_on = models.DateTimeField(
        null=True,
        blank=True
    )
    size = models.PositiveBigIntegerField(default=0)
    cache_dir = models.TextField(
        null=True,
        blank=True,
        help_text="Path to the cache directory for this source file."
    )

    class Meta:
        """Meta class for DataSourceFileCache."""

        constraints = [
            models.UniqueConstraint(
                fields=['source_file', 'hostname'],
                name='source_hostname'
            )
        ]


class DataSourceFileRententionConfig(models.Model):
    """Model representing retention configuration for DataSourceFile."""

    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE
    )
    days_to_keep = models.PositiveIntegerField(
        default=1,
        help_text="Number of days to keep the files before deletion."
    )

    class Meta:
        """Meta class for DataSourceFileRententionConfig."""

        unique_together = ('dataset',)  # Ensure one config per dataset
