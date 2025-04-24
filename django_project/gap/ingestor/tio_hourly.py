# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Term Hourly ingestor.
"""

import logging
from datetime import date

from gap.models import Provider, DatasetType, DatasetTimeStep
from gap.models import (
    CastType, CollectorSession, DatasetStore, Dataset
)
from gap.ingestor.tio_shortterm import (
    TioShortTermDuckDBCollector
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
