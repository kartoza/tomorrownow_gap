# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities Tasks.
"""

from celery.utils.log import get_task_logger

from gap.models.measurement import Measurement
from gap.models.station import Station, StationHistory

logger = get_task_logger(__name__)


def _assign_station_history_to_measurement(history: StationHistory):
    """Assign station history to measurement."""
    logger.info(f'Assign {history.id}')
    Measurement.objects.filter(
        station_id=history.station_id,
        date_time=history.date_time,
        station_history__isnull=True
    ).update(station_history_id=history.id)


def assign_history_of_station_to_measurement(station: Station):
    """Assign station history of a station to measurement."""
    logger.info(f'Assign {station.code}')
    for history in station.stationhistory_set.all():
        _assign_station_history_to_measurement(history)
