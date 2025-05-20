# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tomorrow.io Data Reader
"""

import json
import logging
from datetime import datetime
from typing import List

import pytz
import numpy as np
import pandas as pd
import regionmask
import xarray as xr
from shapely.geometry import shape
from xarray.core.dataset import Dataset as xrDataset

from gap.models import (
    Dataset,
    DatasetAttribute,
    DatasetStore,
    DataSourceFile
)
from gap.providers.base import BaseReaderBuilder
from gap.utils.reader import (
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseDatasetReader
)
from gap.utils.zarr import BaseZarrReader
from core.utils.date import closest_leap_year

logger = logging.getLogger(__name__)
PROVIDER_NAME = 'Tamsat'


class TamsatReaderValue(DatasetReaderValue):
    """Class that convert Tamsat Zarr Dataset to TimelineValues."""

    date_variable = 'date'

    def __init__(
        self, val: xrDataset | List[DatasetTimelineValue],
        location_input: DatasetReaderInput,
        attributes: List[DatasetAttribute],
        closest_leap_year
    ) -> None:
        """Initialize TamsatReaderValue class.

        :param val: value that has been read
        :type val: xrDataset | List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        self.closest_leap_year = closest_leap_year
        super().__init__(val, location_input, attributes)

    def _post_init(self):
        """Override post init method to process the data."""
        super()._post_init()
        base_date = pd.Timestamp(f'{self.closest_leap_year}-01-01')
        date_series = base_date + pd.to_timedelta(
            self._val['dayofyear'].to_series() - 1,
            unit='D'
        )
        # Format as 'MM-DD'
        date_series = date_series.dt.strftime('%m-%d')
        # swap coordinate dayofyear with date
        self._val['dayofyear'] = date_series
        self._val = self._val.rename({'dayofyear': 'date'})
        attrs = {}
        for attr in self.attributes:
            attrs[attr.attribute.variable_name] = list(
                self._val[attr.attribute.variable_name].attrs.keys()
            )
        for key, attr_names in attrs.items():
            for attr_name in attr_names:
                del self._val[key].attrs[attr_name]

    def _xr_dataset_process_datetime(self, df):
        """Process datetime for df from xarray dataset."""
        # do nothing
        return df


class TamsatZarrReader(BaseZarrReader):
    """Tamsat Zarr Reader."""

    date_variable = 'dayofyear'
    datetime_precision = 'D'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime
    ) -> None:
        """Initialize TioZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date
        )
        self.today = datetime.now(tz=pytz.UTC)
        self.closest_leap_year = closest_leap_year(self.today.year)

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        self.setup_reader()
        self.xrDatasets = []
        zarr_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            is_latest=True
        ).order_by('id').last()
        if zarr_file is None:
            return
        ds = self.open_dataset(zarr_file)
        val = self.read_variables(ds, start_date, end_date)
        if val is not None:
            self.xrDatasets.append(val)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return TamsatReaderValue(
            self.xrDatasets[0],
            self.location_input,
            self.attributes,
            self.closest_leap_year
        )

    def _get_day_of_year(self, dt: np.datetime64) -> int:
        """Get day of year for a given date.

        :param date: date to get day of year with D precision
        :type date: np.datetime64
        :return: day of year
        :rtype: int
        """
        return (
            (dt - dt.astype('datetime64[Y]')).astype(int) + 1
        )

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        point = self.location_input.point
        min_idx = self._get_day_of_year(start_dt)
        max_idx = self._get_day_of_year(end_dt)

        return dataset[variables].sel(
            **{self.date_variable: slice(min_idx, max_idx)}
        ).sel(
            lat=point.y,
            lon=point.x, method='nearest')

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        points = self.location_input.points
        lat_min = points[0].y
        lat_max = points[1].y
        lon_min = points[0].x
        lon_max = points[1].x
        min_idx = self._get_day_of_year(start_dt)
        max_idx = self._get_day_of_year(end_dt)
        # output results is in two dimensional array
        return dataset[variables].sel(
            lat=slice(lat_min, lat_max),
            lon=slice(lon_min, lon_max),
            **{self.date_variable: slice(min_idx, max_idx)}
        )

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # Convert the polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)
        # Mask the dataset
        min_idx = self._get_day_of_year(start_dt)
        max_idx = self._get_day_of_year(end_dt)
        return dataset[variables].sel(
            **{self.date_variable: slice(min_idx, max_idx)}
        ).where(
            mask == 0,
            drop=True
        )

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # use the 0 index for it's date variable
        mask = np.zeros_like(dataset[variables[0]][0][0], dtype=bool)

        # Iterate through the points and update the mask
        for lon, lat in self.location_input.points:
            # Find nearest lat and lon indices
            lat_idx = np.abs(dataset['lat'] - lat).argmin()
            lon_idx = np.abs(dataset['lon'] - lon).argmin()
            mask[lat_idx, lon_idx] = True
        mask_da = xr.DataArray(
            mask,
            coords={
                'lat': dataset['lat'], 'lon': dataset['lon']
            }, dims=['lat', 'lon']
        )

        min_idx = self._get_day_of_year(start_dt)
        max_idx = self._get_day_of_year(end_dt)
        # Apply the mask to the dataset
        return dataset[variables].sel(
            **{self.date_variable: slice(min_idx, max_idx)}
        ).where(
            mask_da,
            drop=True
        )


class TamsatReaderBuilder(BaseReaderBuilder):
    """Class to build Tamsat reader."""

    def build(self) -> BaseDatasetReader:
        """Build a new Dataset Reader."""
        return TamsatZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )
