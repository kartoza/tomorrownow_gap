# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities Tasks.
"""

from celery.utils.log import get_task_logger

from core.celery import app
from gap.models.station import Station
from gap.utils.station import assign_history_of_station_to_measurement

logger = get_task_logger(__name__)


@app.task(name='assign_history_of_stations_to_measurement')
def assign_history_of_stations_to_measurement(ids: [int]):
    """Assign stations history of a station to measurement."""
    for station in Station.objects.filter(id__in=ids):
        assign_history_of_station_to_measurement(station)
