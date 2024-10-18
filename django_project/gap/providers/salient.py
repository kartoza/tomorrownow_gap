# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient Data Reader
"""

import json
from typing import List
from datetime import datetime
import numpy as np
import regionmask
import xarray as xr
import pandas as pd
from xarray.core.dataset import Dataset as xrDataset
from shapely.geometry import shape

from gap.models import (
    Dataset,
    DatasetAttribute,
    DataSourceFile,
    DatasetStore
)

from gap.utils.reader import (
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue
)
from gap.utils.netcdf import (
    BaseNetCDFReader
)
from gap.utils.zarr import BaseZarrReader



class SalientReaderValue(DatasetReaderValue):
    """Class that convert Salient Dataset to TimelineValues."""

    date_variable = 'forecast_day'

    def __init__(
            self, val: xrDataset | List[DatasetTimelineValue],
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            forecast_date: np.datetime64,
            is_from_zarr: bool = True) -> None:
        """Initialize SalientReaderValue class.

        :param val: value that has been read
        :type val: xrDataset | List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        self.forecast_date = forecast_date
        self._is_from_zarr = is_from_zarr
        super().__init__(val, location_input, attributes)

    def _post_init(self):
        if self.is_empty():
            return
        if not self._is_xr_dataset:
            return
        # rename attributes and the forecast_day
        renamed_dict = {}
        if self._is_from_zarr:
            renamed_dict = {
                'forecast_day_idx': 'forecast_day'
            }
        for attr in self.attributes:
            renamed_dict[attr.source] = attr.attribute.variable_name
        self._val = self._val.rename(renamed_dict)

        if self._is_from_zarr:
            # replace forecast_day to actualdates
            initial_date = pd.Timestamp(self.forecast_date)
            forecast_day_timedelta = pd.to_timedelta(
                self._val.forecast_day, unit='D')
            forecast_day = initial_date + forecast_day_timedelta
            self._val = self._val.assign_coords(
                forecast_day=('forecast_day', forecast_day))

    def _xr_dataset_to_dict(self) -> dict:
        """Convert xArray Dataset to dictionary.

        Implementation depends on provider.
        :return: data dictionary
        :rtype: dict
        """
        if self.is_empty():
            return {
                'geometry': json.loads(self.location_input.point.json),
                'data': []
            }
        results: List[DatasetTimelineValue] = []
        for dt_idx, dt in enumerate(
            self.xr_dataset[self.date_variable].values):
            value_data = {}
            for attribute in self.attributes:
                var_name = attribute.attribute.variable_name
                if 'ensemble' in self.xr_dataset[var_name].dims:
                    value_data[var_name] = (
                        self.xr_dataset[var_name].values[:, dt_idx]
                    )
                else:
                    v = self.xr_dataset[var_name].values[dt_idx]
                    value_data[var_name] = (
                        v if not np.isnan(v) else None
                    )
            results.append(DatasetTimelineValue(
                dt,
                value_data,
                self.location_input.point
            ))
        return {
            'geometry': json.loads(self.location_input.point.json),
            'data': [result.to_dict() for result in results]
        }


class SalientNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from Salient provider."""

    date_variable = 'forecast_day'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: (float, float) = None
    ) -> None:
        """Initialize SalientNetCDFReader class.

        :param dataset: Dataset from Salient provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.latest_forecast_date = None

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        self.setup_reader()
        self.xrDatasets = []
        netcdf_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.NETCDF
        ).order_by('id').last()
        if netcdf_file is None:
            return
        ds = self.open_dataset(netcdf_file)
        # get latest forecast date
        self.latest_forecast_date = ds['forecast_date'].values
        val = self.read_variables(ds, start_date, end_date)
        if val is None:
            return
        self.xrDatasets.append(val)

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        point = self.location_input.point
        return dataset[variables].sel(
            lat=point.y,
            lon=point.x, method='nearest').where(
                (dataset[self.date_variable] >= start_dt) &
                (dataset[self.date_variable] <= end_dt),
                drop=True
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
        return dataset[variables].where(
            (dataset.lat >= lat_min) & (dataset.lat <= lat_max) &
            (dataset.lon >= lon_min) & (dataset.lon <= lon_max) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # Convert the Django GIS Polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)
        # Mask the dataset
        return dataset[variables].where(
            (mask == 0) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # use the first variable to get its dimension
        if self._has_ensembles():
            # use 0 idx ensemble and 0 idx forecast_day
            mask = np.zeros_like(dataset[variables[0]][0][0], dtype=bool)
        else:
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
        return dataset[variables].where(
            (mask_da) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        val = None
        if len(self.xrDatasets) > 0:
            val = self.xrDatasets[0]
        return SalientReaderValue(
            val, self.location_input, self.attributes,
            self.latest_forecast_date, False)


class SalientZarrReader(BaseZarrReader, SalientNetCDFReader):
    """Salient Zarr Reader."""

    date_variable = 'forecast_day_idx'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: (float, float) = None
    ) -> None:
        """Initialize SalientZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
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
        # get latest forecast date
        self.latest_forecast_date = ds['forecast_date'][-1].values
        if np.datetime64(start_date) < self.latest_forecast_date:
            return
        val = self.read_variables(ds, start_date, end_date)
        if val is None:
            return
        self.xrDatasets.append(val)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        val = None
        if len(self.xrDatasets) > 0:
            val = self.xrDatasets[0]
        return SalientReaderValue(
            val, self.location_input, self.attributes,
            self.latest_forecast_date)

    def _get_forecast_day_idx(self, date: np.datetime64) -> int:
        return int(
            abs((date - self.latest_forecast_date) / np.timedelta64(1, 'D'))
        )

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from single point.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        point = self.location_input.point
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date,
            **{self.date_variable: slice(min_idx, max_idx)}
        ).sel(
            lat=point.y,
            lon=point.x, method='nearest')

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from a bbox.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        points = self.location_input.points
        lat_min = points[0].y
        lat_max = points[1].y
        lon_min = points[0].x
        lon_max = points[1].x
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        # output results is in two dimensional array
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date,
            lat=slice(lat_min, lat_max),
            lon=slice(lon_min, lon_max),
            **{self.date_variable: slice(min_idx, max_idx)}
        )

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from a polygon.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        # Convert the Django GIS Polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)
        # Mask the dataset
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date,
            **{self.date_variable: slice(min_idx, max_idx)}
        ).where(
            mask == 0,
            drop=True
        )

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from a list of point.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        if self._has_ensembles():
            # use 0 idx ensemble and 0 idx forecast_day
            mask = np.zeros_like(dataset[variables[0]][0][0][0], dtype=bool)
        else:
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
        # Apply the mask to the dataset
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date,
            **{self.date_variable: slice(min_idx, max_idx)}
        ).where(
            mask_da,
            drop=True
        )
