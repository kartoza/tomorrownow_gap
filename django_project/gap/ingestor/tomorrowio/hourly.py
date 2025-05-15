# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term Collector for Hourly.
"""

import logging

from gap.models import (
    CastType, CollectorSession, DatasetStore, Dataset, DatasetTimeStep,
    Provider, DatasetType
)
from gap.ingestor.tomorrowio.daily import TioShortTermDailyCollector


logger = logging.getLogger(__name__)


class TioShortTermHourlyCollector(TioShortTermDailyCollector):
    """Collector for Tomorrow.io Short Term Hourly data."""

    CANCEL_KEY_TEMPLATE = '{}_cancel_{}'
    INGESTOR_NAME = 'tio_short_term_hourly'
    TIME_STEP = DatasetTimeStep.HOURLY
    DEFAULT_BATCH_SIZE = 200
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RATE_LIMIT_PER_SECOND = 80
    DEFAULT_MAX_CONCURRENT_REQUESTS = 50
    DEFAULT_BASE_URL = 'https://api.tomorrow.io/v4'

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
