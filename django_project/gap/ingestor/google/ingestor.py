# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor for Google Nowcast Dataset.
"""

from datetime import datetime, timezone
import logging
import os
import json
import time
import rasterio
import rioxarray
import pandas as pd
import xarray as xr
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage

from gap.models import (
    Dataset, DatasetStore, IngestorSessionStatus, CollectorSession,
    DatasetAttribute
)
from core.utils.s3 import s3_compatible_env
from gap.utils.zarr import BaseZarrReader
from gap.ingestor.base import BaseZarrIngestor
from gap.ingestor.exceptions import (
    MissingCollectorSessionException,
    FileNotFoundException
)
from gap.utils.dask import execute_dask_compute
from gap.ingestor.google.common import (
    get_forecast_target_time_from_filename
)

logger = logging.getLogger(__name__)


def cog_to_xarray_advanced(
    filepath, chunks=None, reproject_to_wgs84=True,
    separate_bands=True, band_names=None, verbose=False,
    add_variable_metadata=True
):
    """
    Convert COG to xarray with band metadata and chunking support.

    Parameters:
    -----------
    filepath : str
        Path to the COG file
    chunks : dict, int, tuple, or 'auto', optional
        Chunk sizes for dask arrays. Examples:
        - {'x': 1024, 'y': 1024, 'band': 1} - specific sizes per dimension
        - 1024 - same size for all spatial dimensions
        - (1, 1024, 1024) - tuple for (band, y, x) dimensions
        - 'auto' - let dask determine optimal chunk size
        - None - load entire array into memory (default)
    reproject_to_wgs84 : bool, default=True
        Whether to reproject to EPSG:4326 (lat/lon coordinates)
    separate_bands : bool, default=True
        If True, convert each band to a separate data variable
        If False, keep bands as a dimension
    band_names : list of str, optional
        Custom names for bands. If None, uses band descriptions from file
        or generic names (band_1, band_2, etc.)
    verbose : bool, default=False
        If True, print additional debug information
    add_variable_metadata : bool, default=True
        If True, adds band metadata as attributes to each variable

    Returns:
    --------
    xarray.Dataset
        Dataset with proper coordinates and metadata
    """
    forecast_target_time = get_forecast_target_time_from_filename(
        os.path.basename(filepath)
    )
    time_coords = pd.to_datetime(forecast_target_time, unit='s')
    if verbose:
        logger.info(
            f"Processing file: {filepath} with target time {time_coords}"
        )
    # First, get metadata using rasterio
    with rasterio.open(filepath) as src:
        # Get band descriptions
        band_descriptions = []
        for i in range(src.count):
            desc = src.descriptions[i]
            if desc:
                # Clean up band description for use as variable name
                desc = desc.replace(' ', '_').replace('-', '_')
                # Remove any special characters that might cause issues
                desc = ''.join(
                    c if c.isalnum() or c == '_' else '_' for c in desc
                )
                band_descriptions.append(desc)
            else:
                band_descriptions.append(f'band_{i + 1}')

        # Use custom band names if provided
        if band_names is not None:
            if len(band_names) != src.count:
                raise ValueError(
                    f"Number of band_names ({len(band_names)}) must match "
                    f"number of bands ({src.count})"
                )
            band_descriptions = band_names

        # Get band tags/metadata
        band_tags = [src.tags(i + 1) for i in range(src.count)]

        # Get nodata value
        nodata = src.nodata

        # Get bounds in original CRS
        bounds = src.bounds
        crs = src.crs

        # Get original shape for chunk validation
        height, width = src.height, src.width
        count = src.count
        if verbose:
            # Print some metadata for debugging
            logger.info(f'File: {filepath}')
            logger.info(f'CRS: {crs}')
            logger.info(f'Bounds: {bounds}')
            logger.info(f'Bands: {count}')
            logger.info(f'Band Descriptions: {band_descriptions}')
            logger.info(f'Height: {height}, Width: {width}')

        # Get data type
        dtype = src.dtypes[0]

    # Process chunks parameter
    if chunks is not None:
        if chunks == 'auto':
            processed_chunks = 'auto'
        elif isinstance(chunks, int):
            processed_chunks = {'band': 1, 'x': chunks, 'y': chunks}
        elif isinstance(chunks, tuple) and len(chunks) == 3:
            processed_chunks = {
                'band': chunks[0], 'y': chunks[1], 'x': chunks[2]
            }
        elif isinstance(chunks, dict):
            processed_chunks = chunks.copy()
            if 'band' not in processed_chunks:
                processed_chunks['band'] = 1
        else:
            processed_chunks = chunks

        # Validate chunk sizes
        if isinstance(processed_chunks, dict):
            if 'x' in processed_chunks and processed_chunks['x'] > width:
                processed_chunks['x'] = width
            if 'y' in processed_chunks and processed_chunks['y'] > height:
                processed_chunks['y'] = height
            if (
                'band' in processed_chunks and
                processed_chunks['band'] > count
            ):
                processed_chunks['band'] = count

        if verbose:
            logger.info(f"Using chunks: {processed_chunks}")

        # Open with rioxarray and chunking
        ds = rioxarray.open_rasterio(filepath, chunks=processed_chunks)
    else:
        # Open without chunking
        ds = rioxarray.open_rasterio(filepath)

    # Set nodata value if exists
    if nodata is not None:
        ds = ds.where(ds != nodata)

    # Store original CRS info
    original_attrs = {
        'original_crs': str(crs),
        'original_bounds': bounds,
        'nodata': nodata,
        'dtype': str(dtype)
    }

    # Reproject to lat/lon if requested
    if reproject_to_wgs84 and str(ds.rio.crs) != 'EPSG:4326':
        logger.info(f"Reprojecting from {ds.rio.crs} to EPSG:4326...")
        ds = ds.rio.reproject("EPSG:4326")

    # Rename spatial dimensions for clarity
    dim_names = {}
    if reproject_to_wgs84:
        dim_names = {'x': 'lon', 'y': 'lat'}
    else:
        dim_names = {'x': 'x', 'y': 'y'}

    if separate_bands:
        # Convert each band to a separate data variable
        data_vars = {}

        for i in range(count):
            # Get band data - isel automatically removes the band dimension
            band_data = ds.isel(band=i)

            # Check if band dimension still exists
            # (shouldn't happen with isel, but just in case)
            if 'band' in band_data.dims:
                band_data = band_data.squeeze('band', drop=True)
            elif 'band' in band_data.coords:
                band_data = band_data.drop_vars('band')

            if 'spatial_ref' in band_data.coords:
                band_data = band_data.drop_vars('spatial_ref')

            # Rename dimensions
            band_data = band_data.rename(dim_names)

            # Add time dimension
            band_data = band_data.expand_dims(time=[time_coords])

            # Add band-specific metadata as attributes
            band_attrs = {
                'band_number': i + 1,
                'description': (
                    src.descriptions[i] if i < len(src.descriptions) and
                    src.descriptions[i] else None
                )
            }

            # Add band tags if they exist
            if i < len(band_tags) and band_tags[i]:
                band_attrs['metadata'] = band_tags[i]

            # Preserve CRS information for each band
            if hasattr(band_data, 'rio'):
                band_attrs['crs'] = (
                    str(band_data.rio.crs) if band_data.rio.crs else None
                )

            if add_variable_metadata:
                band_data.attrs.update(band_attrs)
            else:
                band_data = band_data.drop_attrs()

            # Add to data variables
            data_vars[band_descriptions[i]] = band_data

        # Create new dataset with bands as variables
        result_ds = xr.Dataset(data_vars)

        # Add global attributes
        result_ds.attrs.update(original_attrs)

        # Add band metadata to global attributes for reference
        result_ds.attrs['band_names'] = band_descriptions
        result_ds.attrs['number_of_bands'] = count
    else:
        # Keep bands as dimension (original behavior)
        ds = ds.rename(dim_names)
        ds = ds.assign_coords(band=band_descriptions)

        # Add band metadata as attributes
        for i, tags in enumerate(band_tags):
            if tags:
                ds.attrs[f'band_{i + 1}_metadata'] = tags

        ds.attrs.update(original_attrs)
        result_ds = ds

    # Add chunking info to attributes if using dask
    if chunks is not None:
        # Print info about the dataset
        if verbose:
            if separate_bands:
                logger.info(
                    f"Created dataset with {len(result_ds.data_vars)} "
                    "band variables:"
                )
                for var_name in result_ds.data_vars:
                    var = result_ds[var_name]
                    if hasattr(var.data, 'chunks'):
                        logger.info(
                            f"  - {var_name}: shape {var.shape}, "
                            f"chunks {var.data.chunks}"
                        )
            else:
                logger.info(
                    f"Created dask array with shape: {result_ds.dims}"
                )

    return result_ds


class GoogleNowcastIngestor(BaseZarrIngestor):
    """Ingestor for Google Nowcast Dataset."""

    default_chunks = {
        'time': 50,
        'lat': 150,
        'lon': 110
    }

    def __init__(self, session, working_dir):
        """Initialize GoogleNowcastIngestor."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()
        # init variables
        self.variables = list(
            DatasetAttribute.objects.filter(
                dataset=self.dataset
            ).values_list(
                'source', flat=True
            )
        )

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Google Nowcast | 12-hour Forecast',
            store_type=DatasetStore.ZARR
        )

    def _get_encoding(self):
        """Get encoding for dataset variables."""
        encoding = {
            'time': {
                'chunks': self.default_chunks['time']
            }
        }
        for var in self.variables:
            encoding[var] = {
                'chunks': (
                    self.default_chunks['time'],
                    self.default_chunks['lat'],
                    self.default_chunks['lon']
                )
            }
        return encoding

    def _run(self):
        """Process the Google Nowcast dataset."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)

        data_sources = collector.dataset_files.all().order_by(
            'metadata__forecast_target_time'
        )
        if data_sources.count() == 0:
            logger.warning(
                f"No data sources found for collector {collector.id} "
                f"in session {self.session.id}"
            )
            raise FileNotFoundException()

        for data_source in data_sources:
            remote_path = data_source.metadata['remote_url']
            remote_path = (
                f"s3://{self.s3.get('S3_BUCKET_NAME')}/{remote_path}"
            )

            logger.info(f"Processing file: {remote_path}")
            start_time = time.time()
            progress = self._add_progress(os.path.basename(remote_path))
            try:
                with s3_compatible_env(
                    access_key=self.s3.get('S3_ACCESS_KEY_ID'),
                    secret_key=self.s3.get('S3_SECRET_ACCESS_KEY'),
                    endpoint_url=self.s3.get('S3_ENDPOINT_URL'),
                    region=self.s3.get('S3_REGION_NAME')
                ):
                    ds = cog_to_xarray_advanced(
                        remote_path,
                        chunks='auto',
                        reproject_to_wgs84=True,
                        separate_bands=True,
                        band_names=None,
                        verbose=self.get_config('verbose', False),
                        add_variable_metadata=self.created
                    )

                    # save the dataset to Zarr
                    zarr_url = (
                        BaseZarrReader.get_zarr_base_url(self.s3) +
                        self.datasource_file.name
                    )
                    if self.created:
                        x = ds.to_zarr(
                            zarr_url,
                            mode='w',
                            consolidated=True,
                            encoding=self._get_encoding(),
                            storage_options=self.s3_options,
                            compute=False
                        )
                        self.created = False
                    else:
                        x = ds.to_zarr(
                            zarr_url,
                            mode='a',
                            append_dim='time',
                            consolidated=True,
                            storage_options=self.s3_options,
                            compute=False
                        )

                # execute dask compute to finalize the dataset
                execute_dask_compute(x)

                # update progress
                total_time = time.time() - start_time
                progress.notes = f"Execution time: {total_time}"
                progress.status = IngestorSessionStatus.SUCCESS
                progress.save()
            except Exception as e:
                logger.error(f"Failed to process {remote_path}: {e}")
                progress.notes = str(e)
                progress.status = IngestorSessionStatus.FAILED
                progress.save()
                raise e

        # update start/end datetime of zarr datasource file
        self.datasource_file.start_date_time = datetime.fromtimestamp(
            data_sources.first().metadata['forecast_target_time'],
            tz=timezone.utc
        )
        self.datasource_file.end_date_time = datetime.fromtimestamp(
            data_sources.last().metadata['forecast_target_time'],
            tz=timezone.utc
        )
        self.datasource_file.save()

        # remove temporary source file
        remove_temp_file = self.get_config('remove_temp_file', True)
        if remove_temp_file:
            self._remove_source_files(collector)

    def _remove_source_files(self, collector: CollectorSession):
        s3_storage: S3Boto3Storage = storages["gap_products"]
        for dataset_file in collector.dataset_files.all():
            remote_path = dataset_file.metadata['remote_url']
            s3_storage.delete(remote_path)
            dataset_file.delete()

    def run(self):
        """Run Google NowCast Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error('Ingestor Google NowCast failed!', exc_info=True)
            raise e
