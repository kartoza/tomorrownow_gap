# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models

from core.models.common import Definition
from gap.models.common import Provider


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


class DatasetStore:
    """Dataset Storing Type."""

    TABLE = 'TABLE'
    NETCDF = 'NETCDF'
    ZARR = 'ZARR'
    EXT_API = 'EXT_API'


class DatasetTimeStep:
    """Dataset Time Step."""

    HOURLY = 'HOURLY'
    DAILY = 'DAILY'


class Dataset(Definition):
    """Model representing dataset of measument collection."""

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
        ),
        max_length=512
    )
    store_type = models.CharField(
        choices=(
            (DatasetStore.TABLE, DatasetStore.TABLE),
            (DatasetStore.NETCDF, DatasetStore.NETCDF),
            (DatasetStore.ZARR, DatasetStore.ZARR),
            (DatasetStore.EXT_API, DatasetStore.EXT_API),
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
        ),
        max_length=512
    )
