# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Function to convert Google Nowcast Dataset COG files.
"""

import logging
import os
import rasterio
import rioxarray
import pandas as pd
import xarray as xr

from gap.ingestor.google.common import (
    get_forecast_target_time_from_filename
)

logger = logging.getLogger(__name__)


def cog_to_xarray_advanced(
    filepath, chunks=None, reproject_to_wgs84=True,
    separate_bands=True, band_names=None,
    add_variable_metadata=True, ensure_ascending_coords=True,
    verbose=False
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
    add_variable_metadata : bool, default=True
        If True, adds band metadata as attributes to each variable
    ensure_ascending_coords : bool, default=True
        If True, ensures that coordinates are in ascending order
    verbose : bool, default=False
        If True, print additional debug information

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

    # Rename dimensions first
    ds = ds.rename(dim_names)

    # Ensure ascending coordinates if requested
    if ensure_ascending_coords:
        # Check and fix each spatial dimension
        for dim in ds.dims:
            if dim in ['lat', 'lon', 'x', 'y']:
                coord_values = ds[dim].values
                if len(coord_values) > 1:
                    # Check if coordinates are descending
                    if coord_values[0] > coord_values[-1]:
                        if verbose:
                            logger.info(
                                f"Reversing {dim} to ensure "
                                "ascending order..."
                            )
                        ds = ds.sortby(dim)

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

            # Add time dimension
            band_data = band_data.expand_dims(time=[time_coords])

            # Add band-specific metadata as attributes
            band_attrs = {
                'band_number': i + 1,
                'description': (
                    src.descriptions[i] if i < len(src.descriptions) and
                    src.descriptions[i] else band_descriptions[i]
                )
            }

            # Add band tags if they exist
            if i < len(band_tags) and band_tags[i]:
                band_attrs['metadata'] = band_tags[i]

            # Preserve CRS information for each band
            if hasattr(band_data, 'rio'):
                band_attrs['crs'] = (
                    str(band_data.rio.crs) if band_data.rio.crs else str(crs)
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
