# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Google NowCast Data Reader
"""

import json
import logging
from datetime import datetime
from typing import List
from functools import cached_property

import numpy as np
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

logger = logging.getLogger(__name__)
PROVIDER_NAME = 'Google'


class GoogleNowCastReaderValue(DatasetReaderValue):
    """Class that convert Google NowCast Dataset to outputs."""

    date_variable = 'datetime'

    def __init__(
        self, val: xrDataset | List[DatasetTimelineValue],
        location_input: DatasetReaderInput,
        attributes: List[DatasetAttribute]
    ) -> None:
        """Initialize GoogleNowCastReaderValue class.

        :param val: value that has been read
        :type val: xrDataset | List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(val, location_input, attributes)

    @cached_property
    def has_time_column(self) -> bool:
        """Get if the output has time column."""
        return False

    def _post_init(self):
        if not self._is_xr_dataset:
            return
        if self.is_empty():
            return
        renamed_dict = {
            'time': self.date_variable
        }
        for attr in self.attributes:
            renamed_dict[attr.source] = attr.attribute.variable_name
        self._val = self._val.rename(renamed_dict)

    def _xr_dataset_process_datetime(self, df):
        """Process datetime for df from xarray dataset."""
        # do nothing
        return df


class GoogleNowcastZarrReader(BaseZarrReader):
    """Google Nowcast Zarr Reader."""

    date_variable = 'time'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime, use_cache: bool = True
    ) -> None:
        """Initialize GoogleNowcastZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            use_cache=use_cache
        )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return GoogleNowCastReaderValue(
            self.xrDatasets[0],
            self.location_input,
            self.attributes
        )

    def read_forecast_data(self, start_date, end_date):
        """Read forecast data from dataset."""
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

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        point = self.location_input.point

        return dataset[variables].sel(
            **{self.date_variable: slice(start_dt, end_dt)}
        ).sel(
            lat=point.y,
            lon=point.x, method='nearest'
        )

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        points = self.location_input.points
        lat_min = points[0].y
        lat_max = points[1].y
        lon_min = points[0].x
        lon_max = points[1].x
        # output results is in two dimensional array
        return dataset[variables].sel(
            lat=slice(lat_min, lat_max),
            lon=slice(lon_min, lon_max),
            **{self.date_variable: slice(start_dt, end_dt)}
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
        return dataset[variables].sel(
            **{self.date_variable: slice(start_dt, end_dt)}
        ).where(
            mask == 0,
            drop=True
        )

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # use the 0 index for it's date variable
        mask = np.zeros_like(dataset[variables[0]][0], dtype=bool)

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

        # Apply the mask to the dataset
        return dataset[variables].sel(
            **{self.date_variable: slice(start_dt, end_dt)}
        ).where(
            mask_da,
            drop=True
        )


class GoogleReaderBuilder(BaseReaderBuilder):
    """Class to build Google reader."""

    def build(self) -> BaseDatasetReader:
        """Build a new Dataset Reader."""
        return GoogleNowcastZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date, use_cache=self.use_cache
        )
