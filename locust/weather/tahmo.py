# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo User Class for Locust Load Testing
"""

import datetime
from locust import task

from common.base_user import BaseUserScenario
from common.api import ApiWeatherGroupMode


class TahmoWeatherUser(BaseUserScenario):
    """User scenario that accessing Tahmo ground observation data."""

    product_type = 'tahmo_ground_observation'
    attributes = [
        'precipitation',
        'solar_radiation',
        'max_relative_humidity',
        'min_relative_humidity',
        'average_air_temperature',
        'max_air_temperature',
        'min_air_temperature'
    ]

    # dates
    min_date = datetime.date(2019, 1, 1)
    max_date = datetime.date(2023, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 5 * 365 + 1
    default_dates = (
        datetime.date(2019, 3, 1),
        datetime.date(2019, 3, 15),
    )

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'csv'

    @task
    def tahmo_bbox(self):
        """Test with random date."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE
        ]
        dates1 = self.get_random_date_range()

        # test with random variables
        self.api.weather(
            self.product_type,
            self.default_output_type,
            self.attributes[0:3],
            dates1[0],
            dates1[1],
            bbox='33.9, -4.67, 41.89, 5.5',
            group_modes=group_modes
        )
