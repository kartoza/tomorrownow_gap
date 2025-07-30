# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for Zarr.
"""

import fsspec
import xarray as xr

from config import get_settings


def open_zarr_dataset(path):
    """Open zarr dataset."""
    settings = get_settings()
    zarr_url = f's3://{settings.S3_BUCKET_NAME}/{path}'

    s3_options = {
        'key': settings.S3_ACCESS_KEY_ID,
        'secret': settings.S3_SECRET_ACCESS_KEY,
        'client_kwargs': {
            'endpoint_url': settings.S3_ENDPOINT_URL,
        }
    }

    if settings.S3_REGION_NAME:
        s3_options['client_kwargs']['region_name'] = settings.S3_REGION_NAME

    print(f'Opening Zarr dataset from: {zarr_url}')
    s3_mapper = fsspec.get_mapper(zarr_url, **s3_options)
    return xr.open_zarr(
        s3_mapper,
        consolidated=True
    )
