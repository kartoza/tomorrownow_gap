# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term ingestor.
"""

import logging
import os
import pandas as pd
from datetime import timedelta, datetime

from gap.ingestor.async_collector import AsyncCollector
from gap.ingestor.exceptions import (
    AdditionalConfigNotFoundException
)
from gap.models import (
    CastType, CollectorSession, DatasetStore, Dataset, DatasetTimeStep,
    Provider, DatasetType
)
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset


logger = logging.getLogger(__name__)


class TioShortTermDailyCollector(AsyncCollector):
    """Collector for Tomorrow.io Short Term Daily data."""

    CANCEL_KEY_TEMPLATE = '{}_cancel_{}'
    INGESTOR_NAME = 'tio_short_term_daily'
    TIME_STEP = DatasetTimeStep.DAILY
    # Batch Size to insert data into DuckDB
    DEFAULT_BATCH_SIZE = 500
    # Maximum number of retries for API requests
    DEFAULT_MAX_RETRIES = 3
    # Default rate limit per second
    DEFAULT_RATE_LIMIT_PER_SECOND = 70
    # Default maximum concurrent requests
    DEFAULT_MAX_CONCURRENT_REQUESTS = 30
    # Default base URL
    DEFAULT_BASE_URL = 'https://api.tomorrow.io/v4'
    # DEFAULT MAX SIZE OF QUEUE
    DEFAULT_MAX_QUEUE_SIZE = 1000
    # DIRECTORY FOR REMOTE URL
    DEFAULT_REMOTE_URL_DIR = 'tio_collector'

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioShortTermCollector."""
        super().__init__(session, working_dir)
        self.api_key = os.environ.get('TOMORROW_IO_API_KEY', None)
        if not self.api_key:
            raise AdditionalConfigNotFoundException(
                'TOMORROW_IO_API_KEY not found in environment variables.'
            )

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return tomorrowio_shortterm_forecast_dataset()

    def _init_dates(self, today: datetime):
        """Initialize start and end dates."""
        # Retrieve D-6 to D+14
        # Total days: 21
        self.start_dt = today - timedelta(days=6)
        self.end_dt = today + timedelta(days=15)
        self.forecast_date = today

    def get_payload_for_grid(
        self, grid, start_date: datetime, end_date: datetime
    ):
        """Get payload for Tomorrow.io API request."""
        start_dt = start_date
        if (end_date - start_dt).total_seconds() < 24 * 3600:
            start_dt = start_dt - timedelta(days=1)
        lat = grid['lat']
        lon = grid['lon']
        return {
            'location': f'{lat},{lon}',
            'fields': self.attribute_requests,
            'timesteps': (
                ['1h'] if
                self.TIME_STEP == DatasetTimeStep.HOURLY else ['1d']
            ),
            'units': 'metric',
            'startTime': (
                start_dt.isoformat(
                    timespec='seconds').replace("+00:00", "Z")
            ),
            'endTime': (
                end_date.isoformat(
                    timespec='seconds').replace("+00:00", "Z")
            ),
        }

    def get_api_url(self):
        """Get API URL to fetch data."""
        return f'{self.base_url}/timelines?apikey={self.api_key}'

    def get_dataframe_from_batch(self, batch):
        """Get dataframe from batch of data."""
        results = []
        for grid_data in batch:
            item = grid_data['data']
            data = item.get('data', {})
            timelines = data.get('timelines', [])
            intervals = (
                timelines[0].get('intervals', []) if
                len(timelines) > 0 else []
            )
            for interval in intervals:
                result = {
                    'grid_id': grid_data['grid_id'],
                    'lat': grid_data['lat'],
                    'lon': grid_data['lon'],
                    'datetime': interval.get('startTime')
                }
                result.update(interval.get('values', {}))
                results.append(result)
        return pd.DataFrame(results)


class TioShortTermHourlyCollector(TioShortTermDailyCollector):
    """Collector for Tomorrow.io Short Term Hourly data."""

    CANCEL_KEY_TEMPLATE = '{}_cancel_{}'
    INGESTOR_NAME = 'tio_short_term_hourly'
    TIME_STEP = DatasetTimeStep.HOURLY

    # Batch Size to insert data into DuckDB
    DEFAULT_BATCH_SIZE = 100

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize the collector."""
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

    def _init_dates(self, today: datetime):
        """Initialize start and end dates."""
        # Retrieve 4 days of forecast D+1 to D+4
        # Total days: 4
        self.start_dt = today + timedelta(days=1)
        self.end_dt = today + timedelta(days=5)
        self.forecast_date = today
