# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term Hourly ingestor.
"""

import logging
from datetime import date
import numpy as np
import pandas as pd

from gap.models import Provider, DatasetType, DatasetTimeStep
from gap.models import (
    CastType, CollectorSession, DatasetStore, Dataset
)
from gap.ingestor.tio_shortterm import (
    TioShortTermDuckDBCollector, TioShortTermDuckDBIngestor
)


logger = logging.getLogger(__name__)


class TioHourlyShortTermCollector(TioShortTermDuckDBCollector):
    """Collector for Tio Hourly Short-Term forecast data."""

    TIME_STEP = DatasetTimeStep.HOURLY

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioHourlyShortTermCollector."""
        super().__init__(session, working_dir)

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        provider = Provider.objects.get(name='Tomorrow.io')
        dt_shorttermforecast = DatasetType.objects.get(
            variable_name='cbam_shortterm_hourly_forecast',
            type=CastType.FORECAST
        )
        return Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
            provider=provider,
            type=dt_shorttermforecast,
            store_type=DatasetStore.EXT_API,
            time_step=DatasetTimeStep.HOURLY,
            is_internal_use=True
        )

    def _should_skip_date(self, date: date):
        """Skip insert to table for given date."""
        return date == self.end_dt.date()


class TioHourlyShortTermIngestor(TioShortTermDuckDBIngestor):
    """Ingestor for Tio Hourly Short-Term forecast data."""

    TIME_STEP = DatasetTimeStep.HOURLY
    TRIGGER_DCAS = False
    default_chunks = {
        'forecast_date': 10,
        'forecast_day_idx': 21,
        'time': 24,
        'lat': 20,
        'lon': 20
    }
    variables = [
        'total_rainfall',
        'total_evapotranspiration_flux',
        'temperature',
        'precipitation_probability',
        'humidity',
        'wind_speed',
        'solar_radiation',
        'weather_code',
        'flood_index'
    ]

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioHourlyShortTermIngestor."""
        super().__init__(session, working_dir)

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
            store_type=DatasetStore.ZARR
        )

    def get_empty_shape(self, lat_len, lon_len):
        """Get empty shape for the data.

        :param lat_len: length of latitude
        :type lat_len: int
        :param lon_len: length of longitude
        :type lon_len: int
        :return: empty shape
        :rtype: tuple
        """
        return (
            1,
            self.default_chunks['forecast_day_idx'],
            self.default_chunks['time'],
            lat_len,
            lon_len
        )

    def get_chunks_for_forecast_date(self, is_single_date=True):
        """Get chunks for forecast date."""
        if not is_single_date:
            return (
                self.default_chunks['forecast_date'],
                self.default_chunks['forecast_day_idx'],
                self.default_chunks['time'],
                self.default_chunks['lat'],
                self.default_chunks['lon']
            )
        return (
            1,
            self.default_chunks['forecast_day_idx'],
            self.default_chunks['time'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

    def get_data_var_coordinates(self):
        """Get coordinates for data variables."""
        return ['forecast_date', 'forecast_day_idx', 'time', 'lat', 'lon']

    def get_coordinates(self, forecast_date: date, new_lat, new_lon):
        """Get coordinates for the dataset."""
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        forecast_day_indices = np.arange(-6, 15, 1)
        times = np.array([np.timedelta64(h, 'h') for h in range(24)])
        return {
            'forecast_date': ('forecast_date', forecast_date_array),
            'forecast_day_idx': (
                'forecast_day_idx', forecast_day_indices),
            'time': ('time', times),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }

    def get_region_slices(
        self, forecast_date: date, nearest_lat_indices, nearest_lon_indices
    ):
        """Get region slices for update_by_region method."""
        # open existing zarr
        ds = self._open_zarr_dataset()

        # find index of forecast_date
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        new_forecast_date = forecast_date_array[0]
        forecast_date_idx = (
            np.where(ds['forecast_date'].values == new_forecast_date)[0][0]
        )

        ds.close()

        return {
            'forecast_date': slice(
                forecast_date_idx, forecast_date_idx + 1),
            'forecast_day_idx': slice(None),
            'time': slice(None),
            'lat': slice(
                nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
            'lon': slice(
                nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
        }
