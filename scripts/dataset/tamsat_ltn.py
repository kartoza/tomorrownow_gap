# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tamsat LTN NetCDF Reader.
"""

import xarray as xr
import matplotlib.pyplot as plt


LTN_NETCDF_PATH = 'input/tamsat_fullres_region_climatology_spw.nc'


def plot_tamsat_ltn(ds):
    """Plot TAMSAT LTN data for a specific date and lat lon."""
    plt.plot(ds.sel(
        lon=37,
        lat=0,
        method='nearest')['rfe_filled']
    )
    plt.show()


def sort_dims(ds):
    """Sort dimensions of the dataset to ensure ascending order."""
    for dim in ds.dims:
        if dim in ['lat', 'lon', 'x', 'y']:
            coord_values = ds[dim].values
            if len(coord_values) > 1:
                # Check if coordinates are descending
                if coord_values[0] > coord_values[-1]:
                    print(
                        f"Reversing {dim} to ensure "
                        "ascending order..."
                    )
                    ds = ds.sortby(dim)
    return ds


def convert_to_zarr(ds, output_path):
    """Convert xarray dataset to zarr format."""
    ds = sort_dims(ds)
    subset_ds = ds['rfe_filled']
    subset_ds.attrs.update(ds.attrs)
    subset_ds.to_zarr(output_path, mode='w', consolidated=True, zarr_format=2)
    print(f"Converted dataset saved to {output_path}")


def open_tamsat_ltn_netcdf():
    """Open TAMSAT LTN NetCDF file."""
    ds = xr.open_dataset(LTN_NETCDF_PATH)
    print(f"Opened dataset with dimensions: {ds.dims}")
    return ds



def main():
    """Main function to read and process TAMSAT LTN data."""
    ds = open_tamsat_ltn_netcdf()
    
    # Plot the data for a specific lat/lon
    # plot_tamsat_ltn(ds)
    
    # Convert to zarr format
    output_path = 'output/tamsat_ltn_20250812.zarr'
    # convert_to_zarr(ds, output_path)

    # verify the dataset
    zarr_path = 'output/tamsat_ltn_20250812.zarr'
    ds = xr.open_zarr(zarr_path)
    print(f"Zarr dataset opened with dimensions: {ds.dims}")
    print(ds)
    plot_tamsat_ltn(ds)
