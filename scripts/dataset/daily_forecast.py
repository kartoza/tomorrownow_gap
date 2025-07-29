# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Daily Forecast Dataset Reader.
"""

import numpy as np
import pandas as pd
import xarray as xr
import pytz

from dataset.base import DatasetReader, DatasetTimeStep
from helper.zarr import open_zarr_dataset


class DailyForecastReader(DatasetReader):
    """Daily Forecast Dataset Reader."""

    def read(self):
        """Read the daily forecast dataset."""
        self.xrDatasets = []
        ds = open_zarr_dataset(self.file_path)

        # get latest forecast date
        self.latest_forecast_date = ds['forecast_date'][-1].values

        # split date range
        ranges = self._split_date_range(
            self.start_date, self.end_date,
            pd.Timestamp(self.latest_forecast_date).to_pydatetime().replace(
                tzinfo=pytz.UTC
            )
        )

        if ranges['future']:
            val = self.read_variables(
                ds, ranges['future'][0], ranges['future'][1]
            )
            if val:
                dval = val.drop_vars('forecast_date').rename({
                    'forecast_day_idx': 'date'
                })
                initial_date = pd.Timestamp(self.latest_forecast_date)
                forecast_day_timedelta = pd.to_timedelta(dval.date, unit='D')
                forecast_day = initial_date + forecast_day_timedelta
                dval = dval.assign_coords(date=('date', forecast_day))
                self.xrDatasets.append(dval)

        # Hourly dataset does not have past historical data
        if ranges['past'] and self.dataset_time_step == DatasetTimeStep.DAILY:
            val = self.read_variables(
                ds, ranges['past'][0], ranges['past'][1]
            )
            if val:
                val = val.drop_vars(self.source_date_variable).rename({
                    'forecast_date': 'date'
                })
                # Subtract 1 day from the date coordinate
                val = val.assign_coords(
                    date=val.date - np.timedelta64(1, "D")
                )
                self.xrDatasets.append(val)

    def get_result_df(self):
        """Get the result as a DataFrame."""
        val = None
        if len(self.xrDatasets) == 1:
            val = self.xrDatasets[0]
        elif len(self.xrDatasets) == 2:
            val = xr.concat(self.xrDatasets, dim='date').sortby('date')
            val = val.chunk({'date': 30})

        if val is None:
            return pd.DataFrame()
        
        ds, dim_order, reordered_cols = self._get_df(val)
        df = ds.to_dataframe(dim_order=dim_order)
        df = df[reordered_cols]
        df = df.drop(columns=['lat', 'lon'])
        df = df.reset_index()
        # Replace NaN with None
        df = df.astype(object).where(pd.notnull(df), None)

        return df

    def _get_forecast_day_idx(self, date: np.datetime64) -> int:
        return int(
            abs((date - self.latest_forecast_date) / np.timedelta64(1, 'D'))
        )

    def read_variables(self, ds, start_date, end_date):
        """Read variables from the dataset within the date range."""
        start_dt = np.datetime64(start_date, 'ns')
        end_dt = np.datetime64(end_date, 'ns')
        if start_dt < self.latest_forecast_date:
            return ds[self.attributes].sel(
                forecast_date=slice(
                    start_dt + np.timedelta64(1, 'D'),
                    end_dt + np.timedelta64(1, 'D')
                ),
                **{self.source_date_variable: -1}
            ).sel(
                lat=self.lat,
                lon=self.lon, method='nearest'
            )

        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        return ds[self.attributes].sel(
            forecast_date=self.latest_forecast_date,
            **{self.source_date_variable: slice(min_idx, max_idx)}
        ).sel(
            lat=self.lat,
            lon=self.lon, method='nearest'
        )
