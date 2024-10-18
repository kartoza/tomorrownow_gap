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
from gap.utils.reader import (
    LocationInputType,
    DatasetVariable,
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseDatasetReader
)
from gap.utils.zarr import BaseZarrReader

logger = logging.getLogger(__name__)
PROVIDER_NAME = 'Tomorrow.io'
TIO_VARIABLES = {
    'rainAccumulationSum': DatasetVariable(
        'Rain Accumulation',
        'The accumulated amount of liquid rain',
        'mm', 'total_rainfall'
    ),
    'evapotranspirationSum': DatasetVariable(
        'Evapotranspiration',
        'The combined processes by which water moves from '
        'the earth\'s surface into the atmosphere',
        'mm', 'total_evapotranspiration_flux'
    ),
    'temperatureMax': DatasetVariable(
        'Temperature Max',
        '',
        '°C', 'max_temperature'
    ),
    'temperatureMin': DatasetVariable(
        'Temperature Min',
        '',
        '°C', 'min_temperature'
    )
}
TIO_SHORT_TERM_FORCAST_VARIABLES = {
    'precipitationProbability': DatasetVariable(
        'Precipitation Probability',

        (
            'Probability of precipitation represents the chance of >0.0254 cm '
            '(0.01 in.) of liquid equivalent precipitation at a radius '
            'surrounding a point location over a specific period of time.'
        ),
        '%', 'precipitation_probability'
    ),
    'humidityMax': DatasetVariable(
        'Humidity Maximum',
        'The concentration of water vapor present in the air',
        '%', 'humidity_maximum'
    ),
    'humidityMin': DatasetVariable(
        'Humidity Minimum',
        (
            'The total amount of shortwave radiation received '
            'from above by a surface horizontal to the ground'
        ),
        '%', 'humidity_minimum'
    ),
    'windSpeedAvg': DatasetVariable(
        'Wind speed average',
        (
            'The fundamental atmospheric quantity caused by air moving from '
            'high to low pressure, usually due to changes in temperature '
            '(at 10m)'
        ),
        'm/s', 'wind_speed_avg'
    )
}


def tomorrowio_shortterm_forecast_dataset() -> Dataset:
    """Return dataset object for tomorrow.io Dataset for sort term forecast."""
    provider, _ = Provider.objects.get_or_create(name='Tomorrow.io')
    dt_shorttermforecast, _ = DatasetType.objects.get_or_create(
        name='Short-term Forecast',
        defaults={
            'type': CastType.FORECAST
        }
    )
    ds_forecast, _ = Dataset.objects.get_or_create(
        name=f'{provider.name} {dt_shorttermforecast.name}',
        provider=provider,
        type=dt_shorttermforecast,
        store_type=DatasetStore.EXT_API,
        defaults={
            'time_step': DatasetTimeStep.DAILY,
            'is_internal_use': True
        }
    )
    return ds_forecast


class TomorrowIODatasetReader(BaseDatasetReader):
    """Class to read data from Tomorrow.io API."""

    LONG_TERM_NORMALS_TYPE = 'Long Term Normals (20 years)'
    BASE_URL = 'https://api.tomorrow.io/v4'
    HISTORICAL_MAX_DATES = 30

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime, verbose = False,
            altitudes: (float, float) = None
    ) -> None:
        """Initialize Dataset Reader."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.errors = None
        self.warnings = None
        self.results = []
        self.verbose = verbose

    @classmethod
    def init_provider(cls):
        """Init Tomorrow.io provider and variables."""
        provider, _ = Provider.objects.get_or_create(name='Tomorrow.io')
        dt_historical, _ = DatasetType.objects.get_or_create(
            name='Historical Reanalysis',
            defaults={
                'type': CastType.HISTORICAL
            }
        )
        ds_historical, _ = Dataset.objects.get_or_create(
            name=f'{provider.name} {dt_historical.name}',
            provider=provider,
            type=dt_historical,
            store_type=DatasetStore.EXT_API,
            defaults={
                'time_step': DatasetTimeStep.DAILY,
                'is_internal_use': True
            }
        )
        tomorrowio_shortterm_forecast_dataset()
        dt_ltn, _ = DatasetType.objects.get_or_create(
            name=cls.LONG_TERM_NORMALS_TYPE,
            defaults={
                'type': CastType.HISTORICAL
            }
        )
        ds_ltn, _ = Dataset.objects.get_or_create(
            name=f'{provider.name} {dt_ltn.name}',
            provider=provider,
            type=dt_ltn,
            store_type=DatasetStore.EXT_API,
            defaults={
                'time_step': DatasetTimeStep.DAILY,
                'is_internal_use': True
            }
        )

    def _is_ltn_request(self):
        """Check if the request is for Long Term Normal (LTN) request."""
        return (
                self.dataset.type.type == CastType.HISTORICAL and
                self.dataset.type.name == self.LONG_TERM_NORMALS_TYPE
        )

    def _get_api_key(self):
        """Retrieve API Key for Tomorrow.io."""
        return os.environ.get('TOMORROW_IO_API_KEY', '')

    def _get_headers(self):
        """Get request headers."""
        return {
            'Accept-Encoding': 'gzip',
            'accept': 'application/json',
            'content-type': 'application/json'
        }

    def geom_type_allowed(self):
        """Return if geom type is allowed."""
        return self.location_input.type in [
            LocationInputType.POINT, LocationInputType.POLYGON
        ]

    def _get_payload(
            self, start_date: datetime, end_date: datetime,
            is_ltn: bool = False):
        """Get request payload.

        This method will normalize the start_date if
        start_date and date is less than 24H.

        :param start_date: _description_
        :type start_date: datetime
        :param end_date: _description_
        :type end_date: datetime
        :param is_ltn: _description_, defaults to False
        :type is_ltn: bool, optional
        :return: _description_
        :rtype: _type_
        """
        start_dt = start_date
        if (end_date - start_dt).total_seconds() < 24 * 3600:
            start_dt = start_dt - timedelta(days=1)
        payload = {
            'location': json.loads(self.location_input.geometry.geojson),
            'fields': [attr.source for attr in self.attributes],
            'timesteps': ['1d'],
            'units': 'metric',
        }
        if is_ltn:
            payload.update({
                'startDate': start_dt.strftime('%m-%d'),
                'endDate': end_date.strftime('%m-%d')
            })
        else:
            payload.update({
                'startTime': (
                    start_dt.isoformat(
                        timespec='seconds').replace("+00:00", "Z")
                ),
                'endTime': (
                    end_date.isoformat(
                        timespec='seconds').replace("+00:00", "Z")
                ),
            })
        return payload

    def read(self):
        """Read values from Tomorrow.io API."""
        self.results = []
        self.errors = None
        self.warnings = None
        today = datetime.now(tz=pytz.UTC)
        if not self.geom_type_allowed:
            return
        # handles:
        # - start_date=end_date
        # - d-7 should be using timelines API
        # - historical/timelines may return the same day,
        #   choosing to use historical
        if self._is_ltn_request():
            self._read_ltn_data(
                self.start_date,
                self.end_date
            )
        elif self.dataset.type.type == CastType.HISTORICAL:
            max_date = today - timedelta(days=7)
            if self.start_date < max_date:
                self.read_historical_data(
                    self.start_date,
                    self.end_date if self.end_date < max_date else
                    max_date
                )
            if self.end_date >= max_date:
                # read from forecast data
                start_dt = self.start_date
                if max_date > start_dt:
                    start_dt = max_date + timedelta(days=1)
                self.read_forecast_data(
                    start_dt,
                    self.end_date
                )
        else:
            min_available_timelines = today - timedelta(days=6)
            self.read_forecast_data(
                self.start_date if
                self.start_date >= min_available_timelines else
                min_available_timelines,
                self.end_date
            )

    def _log_errors(self):
        """Log any errors from the API."""
        logger.error(f'Tomorrow.io API errors: {len(self.errors)}')
        logger.error(json.dumps(self.errors))

    def _log_warnings(self):
        """Log any warnings from the API."""
        if not self.verbose:
            return
        logger.warning(f'Tomorrow.io API warnings: {len(self.warnings)}')
        logger.warning(json.dumps(self.warnings))

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        if not self.geom_type_allowed:
            return DatasetReaderValue([], self.location_input, self.attributes)
        if not self.is_success():
            self._log_errors()
            return DatasetReaderValue([], self.location_input, self.attributes)
        if self.warnings:
            self._log_warnings()
        return DatasetReaderValue(
            self.results, self.location_input, self.attributes)

    def _split_historical_date_ranges(
            self, start_date: datetime, end_date: datetime) -> List[dict]:
        """Split date range for historical with max 30 days of each request.

        :param start_date: start date of historical request
        :type start_date: datetime
        :param end_date: end date of historical request
        :type end_date: datetime
        :return: Date ranges with start_date and end_date attribute
        :rtype: List[dict]
        """
        if start_date.date() == end_date.date():
            return [{
                'start_date': start_date,
                'end_date': end_date
            }]
        date_ranges = []
        iter_date = start_date
        while iter_date < end_date:
            iter_end_date = (
                iter_date + timedelta(days=self.HISTORICAL_MAX_DATES)
            )
            if iter_end_date > end_date:
                iter_end_date = end_date
            date_ranges.append({
                'start_date': iter_date,
                'end_date': iter_end_date
            })
            iter_date = iter_end_date
        return date_ranges

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        url = f'{self.BASE_URL}/historical?apikey={self._get_api_key()}'
        date_ranges = self._split_historical_date_ranges(start_date, end_date)
        for date_range in date_ranges:
            payload = self._get_payload(
                date_range['start_date'], date_range['end_date'])
            response = requests.post(
                url, json=payload, headers=self._get_headers())
            if response.status_code != 200:
                self._get_error_from_response(response)
                continue
            self.results.extend(self._parse_result(response.json()))

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        url = f'{self.BASE_URL}/timelines?apikey={self._get_api_key()}'
        payload = self._get_payload(start_date, end_date)
        response = requests.post(
            url, json=payload, headers=self._get_headers())
        if response.status_code != 200:
            self._get_error_from_response(response)
            return
        self.results.extend(self._parse_result(response.json()))

    def _read_ltn_data(self, start_date: datetime, end_date: datetime):
        """Read Long Term Normals (LTN) data.

        :param start_date: start date for reading data
        :type start_date: datetime
        :param end_date: end date for reading data
        :type end_date: datetime
        """
        url = (
            f'{self.BASE_URL}/historical/normals?apikey={self._get_api_key()}'
        )
        payload = self._get_payload(start_date, end_date, is_ltn=True)
        response = requests.post(
            url, json=payload, headers=self._get_headers())
        if response.status_code != 200:
            self._get_error_from_response(response)
            return
        self.results = self._parse_result(response.json())

    def _get_error_from_response(self, response):
        """Get error detail from Tomorrow.io API response.

        :param response: API response
        :type response: response object
        """
        error = "Unknown error!"
        try:
            result = response.json()
            error = f"{result.get('type', '')} {result.get('message', '')}"
        except Exception:
            pass
        if self.errors is None:
            self.errors = [error]
        else:
            self.errors.append(error)

    def _get_result_datetime(self, interval: dict) -> datetime:
        """Parse datetime from API response.

        :param interval: interval dictionary
        :type interval: dict
        :return: datetime
        :rtype: datetime
        """
        dt_str = interval.get('startTime')
        if self._is_ltn_request():
            dt_str = interval.get('startDate')
            dt_str = f'{self.start_date.year}-{dt_str}'
            return datetime.strptime(
                dt_str, '%Y-%m-%d').replace(tzinfo=pytz.utc)
        return datetime.fromisoformat(dt_str)

    def _parse_result(self, result: dict) -> List[DatasetTimelineValue]:
        """Parse successful response from Tomorrow.io API.

        This method also checks for any warnings in the response.
        :param result: response data
        :type result: dict
        :return: data values
        :rtype: List[DatasetTimelineValue]
        """
        value_list = []
        data = result.get('data', {})
        timelines = data.get('timelines', [])
        intervals = (
            timelines[0].get('intervals', []) if len(timelines) > 0 else []
        )
        for interval in intervals:
            start_dt = self._get_result_datetime(interval)
            if start_dt < self.start_date or start_dt > self.end_date:
                continue
            values = interval.get('values')
            value_data = {}
            for attribute in self.attributes:
                value_data[attribute.attribute.variable_name] = (
                    values.get(attribute.source, None)
                )
            value_list.append(DatasetTimelineValue(
                start_dt,
                value_data,
                self.location_input.geometry
            ))
        warnings = data.get('warnings', None)
        if warnings:
            if self.warnings is None:
                self.warnings = warnings
            else:
                self.warnings.extend(warnings)
        return value_list

    def is_success(self) -> bool:
        """Check whether the API requests are successful.

        :return: True if there is no errors
        :rtype: bool
        """
        return self.errors is None

    def get_raw_results(self) -> List[DatasetTimelineValue]:
        """Get the raw results of dataset timeline values.

        :return: list of dataset timeline values
        :rtype: List[DatasetTimelineValue]
        """
        if not self.is_success():
            self._log_errors()
        return self.results


class TioZarrReaderValue(DatasetReaderValue):
    """Class that convert Tio Zarr Dataset to TimelineValues."""

    date_variable = 'forecast_day'

    def __init__(
            self, val: xrDataset | List[DatasetTimelineValue],
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            forecast_date: np.datetime64) -> None:
        """Initialize TioZarrReaderValue class.

        :param val: value that has been read
        :type val: xrDataset | List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        self.forecast_date = forecast_date
        super().__init__(val, location_input, attributes)

    def _post_init(self):
        if self.is_empty():
            return
        if not self._is_xr_dataset:
            return

        # rename attributes and the forecast_day
        renamed_dict = {
            'forecast_day_idx': 'forecast_day'
        }
        for attr in self.attributes:
            renamed_dict[attr.source] = attr.attribute.variable_name
        self._val = self._val.rename(renamed_dict)

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


class TioZarrReader(BaseZarrReader):
    """Tio Zarr Reader."""

    date_variable = 'forecast_day_idx'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: (float, float) = None
    ) -> None:
        """Initialize TioZarrReader class."""
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
        return TioZarrReaderValue(
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
        # Convert the polygon to a format compatible with shapely
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
