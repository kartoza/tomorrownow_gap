# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Generator
"""

import logging
import os
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import List, Tuple

import pytz
from django.contrib.gis.geos import Point, Polygon
from django.utils import timezone

from gap.models import DatasetAttribute, DatasetStore
from gap.providers import TIO_PROVIDER
from spw.models import RModel, RModelExecutionLog, RModelExecutionStatus
from spw.utils.plumber import (
    execute_spw_model,
    write_plumber_data,
    remove_plumber_data,
    PLUMBER_PORT
)
from .gap_input import GapInput

logger = logging.getLogger(__name__)
ATTRIBUTES = [
    'total_evapotranspiration_flux',
    'total_rainfall',
    'max_temperature',
    'min_temperature',
    'precipitation_probability'
]
COLUMNS = [
    'month_day',
    'date',
    'evapotranspirationSum',
    'rainAccumulationSum',
    'LTNPET',
    'LTNPrecip'
]
VAR_MAPPING = {
    'total_evapotranspiration_flux': 'evapotranspirationSum',
    'total_rainfall': 'rainAccumulationSum',
    'max_temperature': 'temperatureMax',
    'min_temperature': 'temperatureMin',
    'precipitation_probability': 'precipitationProbability',
}
VAR_MAPPING_REVERSE = {v: k for k, v in VAR_MAPPING.items()}
LTN_MAPPING = {
    'total_evapotranspiration_flux': 'LTNPET',
    'total_rainfall': 'LTNPrecip'
}


class SPWOutput:
    """Class to store the output from SPW model."""

    def __init__(
            self, point: Point, input_data: dict) -> None:
        """Initialize the SPWOutput class."""
        self.point = point
        data = {}
        for key, val in input_data.items():
            if key == 'metadata':
                continue
            if isinstance(val, list) and len(val) == 1:
                data[key] = val[0]
            else:
                data[key] = val
        self.data = SimpleNamespace(**data)


def calculate_from_point_attrs():
    """Return attributes that are being used in calculate from point."""
    return DatasetAttribute.objects.filter(
        attribute__variable_name__in=ATTRIBUTES,
        dataset__provider__name=TIO_PROVIDER,
        dataset__store_type=DatasetStore.EXT_API
    )


def _calculate_from_point(
        point: Point, port=PLUMBER_PORT
) -> Tuple[SPWOutput, dict]:
    """Calculate from point."""
    today = datetime.now(tz=pytz.UTC)
    start_dt = today - timedelta(days=6)
    end_dt = today + timedelta(days=13)
    logger.info(
        f'Calculate SPW for {point} at Today: {today} - '
        f'start_dt: {start_dt} - end_dt: {end_dt}'
    )

    data_input = GapInput(point.y, point.x, today)
    historical_dict = data_input.get_data()
    rows = data_input.get_spw_data()

    return _execute_spw_model(rows, point, port), historical_dict


def calculate_from_point(
    point: Point, port=PLUMBER_PORT
) -> Tuple[SPWOutput, dict]:
    """Calculate SPW from given point.

    :param point: Location to be queried
    :type point: Point
    :return: Output with GoNoGo classification
    :rtype: Tuple[SPWOutput, dict]
    """
    return _calculate_from_point(point, port)


def calculate_from_polygon(
    polygon: Polygon, port=PLUMBER_PORT
) -> Tuple[SPWOutput, dict]:
    """Calculate SPW from given point.

    :param polygon: Location to be queried
    :type polygon: Polygon
    :return: Output with GoNoGo classification
    :rtype: Tuple[SPWOutput, dict]
    """
    return _calculate_from_point(polygon.centroid, port)


def _execute_spw_model(
    rows: List, point: Point, port=PLUMBER_PORT
) -> SPWOutput:
    """Execute SPW Model and return the output.

    :param rows: Data rows
    :type rows: List
    :param point: location input
    :type point: Point
    :return: SPW Model output
    :rtype: SPWOutput
    """
    model = RModel.objects.order_by('-version').first()
    data_file_path = write_plumber_data(COLUMNS, rows, dir_path='/tmp')
    filename = os.path.basename(data_file_path)
    execution_log = RModelExecutionLog.objects.create(
        model=model,
        location_input=point,
        start_date_time=timezone.now()
    )
    with open(data_file_path, 'rb') as output_file:
        execution_log.input_file.save(filename, output_file)
    remove_plumber_data(data_file_path)
    success, data = execute_spw_model(
        execution_log.input_file.url, filename, point.y, point.x, 'gap_place',
        port=port
    )
    if isinstance(data, dict):
        execution_log.output = data
    else:
        execution_log.errors = data
    output = None
    if success:
        output = SPWOutput(point, data)
    execution_log.status = (
        RModelExecutionStatus.SUCCESS if success else
        RModelExecutionStatus.FAILED
    )
    execution_log.end_date_time = timezone.now()
    execution_log.save()
    return output
