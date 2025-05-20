# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tomorrow.io Data Reader
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import List

import pytz
import requests
import numpy as np
import pandas as pd
import regionmask
import xarray as xr
from shapely.geometry import shape
from xarray.core.dataset import Dataset as xrDataset

from gap.models import (
    Provider,
    CastType,
    DatasetType,
    Dataset,
    DatasetAttribute,
    DatasetTimeStep,
    DatasetStore,
    DataSourceFile
)
from gap.providers.base import BaseReaderBuilder
from gap.utils.reader import (
    LocationInputType,
    DatasetVariable,
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
            attributes: List[DatasetAttribute]) -> None:
        """Initialize TamsatReaderValue class.

        :param val: value that has been read
        :type val: xrDataset | List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(val, location_input, attributes)


class TamsatZarrReader(BaseZarrReader):
    """Tamsat Zarr Reader."""

    date_variable = 'forecast_day_idx'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime
    ) -> None:
        """Initialize TioZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date
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
            self.xrDatasets.append(val)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return TamsatReaderValue(
            self.xrDatasets[0],
            self.location_input,
            self.attributes
        )

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        return None


class TamsatReaderBuilder(BaseReaderBuilder):
    """Class to build Tamsat reader."""

    def build(self) -> BaseDatasetReader:
        """Build a new Dataset Reader."""
        return TamsatZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )
