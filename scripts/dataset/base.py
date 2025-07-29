# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Dataset Reader.
"""

import pandas as pd
from datetime import datetime, timedelta


class DatasetTimeStep:
    """Dataset Time Step."""

    QUARTER_HOURLY = 'QUARTER_HOURLY'
    HOURLY = 'HOURLY'
    DAILY = 'DAILY'
    OTHER = 'OTHER'


class DatasetSourceType:
    """Enum for dataset types."""
    ZARR = 'zarr'
    GEOPARQUET = 'geoparquet'


class DatasetType:
    """Enum for dataset types."""
    DAILY_FORECAST = 'DAILY_FORECAST'


class DatasetReader:
    """Base class for dataset readers."""
    
    dataset_type = DatasetSourceType.ZARR
    dataset_time_step = DatasetTimeStep.DAILY
    source_date_variable = 'forecast_day_idx'
    date_variable = 'date'
    has_time_column = False

    def __init__(self, lat, lon, start_date, end_date, attributes, file_path):
        """Initialize the dataset reader."""
        self.lat = lat
        self.lon = lon
        self.start_date = start_date
        self.end_date = end_date
        self.attributes = attributes
        self.file_path = file_path
        self.xrDatasets = []

    def read(self):
        """Read the dataset from the given path."""
        raise NotImplementedError("Subclasses should implement this method.")

    def get_result_df(self) -> pd.DataFrame:
        """Get the result as a DataFrame."""
        raise NotImplementedError("Subclasses should implement this method.")

    def _split_date_range(
            self, start_date: datetime, end_date: datetime,
            now: datetime
    ) -> dict:
        """Split a date range into past and future ranges."""
        if end_date < now:
            # Entire range is in the past
            return {'past': (start_date, end_date), 'future': None}
        elif start_date >= now:
            # Entire range is in the future
            return {'past': None, 'future': (start_date, end_date)}
        else:
            # Split into past and future
            return {
                'past': (start_date, now - timedelta(days=1)),
                'future': (now, end_date)
            }

    def _get_df(
        self, ds, date_chunk_size=None, lat_chunk_size=None,
        lon_chunk_size=None
    ):
        dim_order = [self.date_variable]

        if self.has_time_column:
            dim_order.append('time')

        reordered_cols = self.attributes
        # use date chunk = 1 to order by date
        rechunk = {
            self.date_variable: date_chunk_size or 1
        }
        if 'lat' in ds.dims:
            dim_order.append('lat')
            dim_order.append('lon')
            rechunk['lat'] = lat_chunk_size or 300
            rechunk['lon'] = lon_chunk_size or 300
            if self.has_time_column:
                # slightly reducing chunk size for lat/lon
                rechunk['lat'] = lat_chunk_size or 100
                rechunk['lon'] = lon_chunk_size or 100
                rechunk['time'] = 24
        else:
            reordered_cols.insert(0, 'lon')
            reordered_cols.insert(0, 'lat')
            rechunk[self.date_variable] = date_chunk_size or 300
            if self.has_time_column:
                # slightly reducing chunk size for lat/lon
                rechunk[self.date_variable] = date_chunk_size or 100
                rechunk['time'] = 24

        if 'ensemble' in ds.dims:
            dim_order.append('ensemble')
            rechunk['ensemble'] = 50

        # rechunk dataset
        result_ds = ds.chunk(rechunk)

        if self.has_time_column:
            time_delta = result_ds['time'].dt.total_seconds().values
            time_str = [
                f"{int(x // 3600):02}:{int((x % 3600) // 60):02}"
                f":{int(x % 60):02}"
                for x in time_delta
            ]
            result_ds = result_ds.assign_coords(
                **{'time': ('time', time_str)}
            )

        return result_ds, dim_order, reordered_cols
