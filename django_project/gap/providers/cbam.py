# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Data Reader
"""

import json
from typing import List
from datetime import datetime
import numpy as np
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import regionmask
from shapely.geometry import shape

from gap.models import (
    Dataset,
    DatasetAttribute,
    DataSourceFile,
    DatasetStore,
    DatasetTimeStep
)
from gap.providers.base import BaseReaderBuilder
from gap.utils.reader import (
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue
)
from gap.utils.netcdf import (
    daterange_inc,
    BaseNetCDFReader
)
from gap.utils.zarr import BaseZarrReader


class CBAMReaderValue(DatasetReaderValue):
    """Class that convert CBAM Dataset to TimelineValues."""

    date_variable = 'date'

    def __init__(
        self, val: xrDataset | List[DatasetTimelineValue],
        location_input: DatasetReaderInput,
        attributes: List[DatasetAttribute],
        start_datetime: np.datetime64 = None,
        end_datetime: np.datetime64 = None
    ) -> None:
        """Initialize CBAMReaderValue class.

        :param val: value that has been read
        :type val: xrDataset | List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(
            val,
            location_input,
            attributes,
            start_datetime=start_datetime,
            end_datetime=end_datetime
        )


class CBAMNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from CBAM provider."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime, use_cache: bool = True
    ) -> None:
        """Initialize CBAMNetCDFReader class.

        :param dataset: Dataset from CBAM provider
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
            dataset, attributes, location_input,
            start_date, end_date, use_cache=use_cache
        )

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        self.setup_reader()
        self.xrDatasets = []
        for filter_date in daterange_inc(start_date, end_date):
            netcdf_file = DataSourceFile.objects.filter(
                dataset=self.dataset,
                start_date_time__gte=filter_date,
                end_date_time__lte=filter_date,
                format=DatasetStore.NETCDF
            ).first()
            if netcdf_file is None:
                continue
            ds = self.open_dataset(netcdf_file)
            val = self.read_variables(ds, filter_date, filter_date)
            if val is None:
                continue
            self.xrDatasets.append(val)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        val = None
        if len(self.xrDatasets) > 1:
            val = xr.combine_nested(
                self.xrDatasets, concat_dim=[self.date_variable])
        elif len(self.xrDatasets) == 1:
            val = self.xrDatasets[0]
        return CBAMReaderValue(val, self.location_input, self.attributes)

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        point = self.location_input.point
        return dataset[variables].sel(
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
        # output results is in two dimensional array
        return dataset[variables].where(
            (dataset.lat >= lat_min) & (dataset.lat <= lat_max) &
            (dataset.lon >= lon_min) & (dataset.lon <= lon_max), drop=True)

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
        return dataset[variables].where(mask == 0, drop=True)

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # use the first variable to get its dimension
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
            },
            dims=['lat', 'lon']
        )
        # Apply the mask to the dataset
        return dataset[variables].where(mask_da, drop=True)


class CBAMZarrReader(BaseZarrReader, CBAMNetCDFReader):
    """CBAM Zarr Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime, use_cache: bool = True
    ) -> None:
        """Initialize CBAMZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            use_cache=use_cache
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
        """Read variables values from BBOX.

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
        """Read variables values from polygon.

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
        # Convert the Django GIS Polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)

        # Mask the dataset
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
        """Read variables values from list of point.

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

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        self.setup_reader()
        zarr_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            is_latest=True
        ).order_by('id').last()
        if zarr_file is None:
            return
        ds = self.open_dataset(zarr_file)
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
        return CBAMReaderValue(val, self.location_input, self.attributes)


class CBAMHourlyForecastHistoricalReader(CBAMZarrReader):
    """Class that represents CBAM Hourly Forecast Historical Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime, use_cache: bool = True
    ) -> None:
        """Initialize CBAMHourlyForecastHistoricalReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            use_cache=use_cache
        )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        val = None
        start_dt = np.datetime64(self.start_date, 'ns')
        end_dt = np.datetime64(self.end_date, 'ns')
        if len(self.xrDatasets) > 0:
            val = self.xrDatasets[0]

        if val is None:
            return CBAMReaderValue(
                val, self.location_input, self.attributes,
                start_dt, end_dt
            )

        if self.has_time_column:
            val = self._mask_by_datetime_range(
                val, start_dt, end_dt
            )

        return CBAMReaderValue(
            val, self.location_input, self.attributes,
            start_dt, end_dt
        )

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        self.setup_reader()
        zarr_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            is_latest=True
        ).order_by('id').last()
        if zarr_file is None:
            return
        ds = self.open_dataset(zarr_file)
        start_dt = start_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_dt = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        val = self.read_variables(ds, start_dt, end_dt)
        if val is None:
            return
        self.xrDatasets.append(val)


class CBAMReaderBuilder(BaseReaderBuilder):
    """CBAM Reader Builder."""

    def build(self) -> CBAMZarrReader:
        """Build a new reader from given dataset.

        :return: Reader Class Type
        :rtype: CBAMZarrReader
        """
        if self.dataset.time_step == DatasetTimeStep.HOURLY:
            # For hourly forecast, we use CBAMHourlyForecastHistoricalReader
            return CBAMHourlyForecastHistoricalReader(
                self.dataset, self.attributes, self.location_input,
                self.start_date, self.end_date, use_cache=self.use_cache
            )
        return CBAMZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date, use_cache=self.use_cache
        )
