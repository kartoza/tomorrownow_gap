# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Class for Locust Load Testing
"""

import datetime
from locust import task

from common.base_user import BaseUserScenario
from common.api import ApiWeatherGroupMode


class CBAMWeatherUser(BaseUserScenario):
    """User scenario that accessing CBAM historical data."""

    product_type = 'cbam_historical_analysis'
    attributes = [
        'min_temperature',
        # 'min_day_temperature',
        'total_rainfall',
        # 'max_day_temperature',
        # 'min_night_temperature',
        'total_solar_irradiance',
        # 'average_solar_irradiance',
        # 'max_night_temperature',
        'max_temperature',
        'total_evapotranspiration_flux'
    ]

    # dates
    min_date = datetime.date(2012, 1, 1)
    max_date = datetime.date(2012, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 366

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'netcdf'

    @task
    def cbam_bbox(self):
        """Test with random date."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE
        ]
        dates1 = self.get_random_date_range()

        # test with random variables
        self.api.weather(
            self.product_type,
            'csv',
            self.attributes[0:4],
            dates1[0],
            dates1[1],
            bbox='37.098879465,0.445798074,39.352208827,2.414724235',
            group_modes=group_modes
        )


class CBAMBiasAdjustWeatherUser(BaseUserScenario):
    """User scenario that accessing CBAM Bias Adjust historical data."""

    product_type = 'cbam_historical_analysis_bias_adjust'
    attributes = [
        'min_temperature',
        'total_rainfall',
        'total_solar_irradiance',
        'max_temperature'
    ]

    # dates
    min_date = datetime.date(2013, 1, 1)
    max_date = datetime.date(2023, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 90

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'netcdf'


class CBAMShortTermWeatherUser(BaseUserScenario):
    """User scenario that accessing CBAM Short-term forecast data."""

    product_type = 'cbam_shortterm_forecast'
    attributes = [
        'total_rainfall',
        'total_evapotranspiration_flux',
        'max_temperature',
        'min_temperature',
        'precipitation_probability',
        'humidity_maximum',
        'humidity_minimum',
        'wind_speed_avg',
        'solar_radiation'
    ]

    # dates
    min_date = datetime.datetime.now().date()
    max_date = (
        datetime.datetime.now().date() + datetime.timedelta(days=14)
    )
    min_rand_dates = 1
    max_rand_dates = 14
    default_dates = (
        min_date,
        max_date,
    )

    # location
    point = (-0.02378, 35.008688,)

    # output
    default_output_type = 'netcdf'
