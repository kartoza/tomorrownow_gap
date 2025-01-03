# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Class for Locust Load Testing
"""

import random
import datetime
from locust import HttpUser, tag, task

from common.auth import auth_config
from common.api import Api, ApiTaskTag, ApiWeatherGroupMode


class BaseUserScenario(HttpUser):
    """Base User scenario for testing API."""

    product_type = ''
    attributes = []
    output_types = [
        'json',
        'csv',
        'netcdf'
    ]

    # dates
    min_date = datetime.date(2013, 1, 1)
    max_date = datetime.date(2023, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 30
    default_dates = (
        datetime.date(2019, 3, 1),
        datetime.date(2019, 3, 15),
    )

    # TODO: generate random?
    # location
    point = (-1.404244, 35.008688,)
    bbox = ''
    location_name = None

    # output
    default_output_type = 'csv'

    def on_start(self):
        """Set the test."""
        self.api = Api(self.client, auth_config.get_user())

    def wait_time(self):
        """Get wait_time in second."""
        return self.api.user['wait_time'](self)

    def _random_date(self, start_date, end_date):
        """Generate random date."""
        # Calculate the difference in days between the two dates
        delta = end_date - start_date
        # Generate a random number of days to add to the start date
        random_days = random.randint(0, delta.days)
        # Return the new random date
        return start_date + datetime.timedelta(days=random_days)

    def _random_attributes(self, attributes):
        """Generate random selection of attributes."""
        # Choose a random number of attributes to select
        # (at least 1, up to the total length of the list)
        num_to_select = random.randint(1, len(attributes))
        # Randomly sample the attributes
        selected_attributes = random.sample(attributes, num_to_select)
        return selected_attributes

    def get_random_date_range(self):
        """Generate random date range."""
        days_count = random.randint(
            self.min_rand_dates, self.max_rand_dates)
        rand_date = self._random_date(
            self.min_date, self.max_date
        )

        rand_max_date = rand_date + datetime.timedelta(days=10)
        return (
            rand_date,
            rand_max_date if rand_max_date <= self.max_date else self.max_date
        )

    def get_random_attributes(self):
        """Generate random attributes."""
        return self._random_attributes(self.attributes)

    def get_random_output_type(self):
        """Generate random output type."""
        return random.sample(self.output_types, 1)[0]


    @tag(ApiTaskTag.RANDOM_VAR)
    @task
    def random_variables(self):
        """Test with random variables."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE,
            ApiWeatherGroupMode.BY_ATTRIBUTE_LENGTH
        ]

        # test with random variables
        self.api.weather(
            self.product_type,
            self.default_output_type,
            self.get_random_attributes(),
            self.default_dates[0],
            self.default_dates[1],
            lat=self.point[0],
            lon=self.point[1],
            group_modes=group_modes
        )

    @tag(ApiTaskTag.RANDOM_DATE)
    @task
    def random_date(self):
        """Test with random date."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE,
            ApiWeatherGroupMode.BY_DATE_COUNT
        ]
        dates1 = self.get_random_date_range()

        # test with random variables
        self.api.weather(
            self.product_type,
            self.default_output_type,
            self.attributes[0:3],
            dates1[0],
            dates1[1],
            lat=self.point[0],
            lon=self.point[1],
            group_modes=group_modes
        )

    @tag(ApiTaskTag.RANDOM_OUTPUT)
    @task
    def random_output(self):
        """Test with random output_type."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE,
            ApiWeatherGroupMode.BY_OUTPUT_TYPE
        ]
        # test with random variables
        self.api.weather(
            self.product_type,
            self.get_random_output_type(),
            self.attributes[0:3],
            self.default_dates[0],
            self.default_dates[1],
            lat=self.point[0],
            lon=self.point[1],
            group_modes=group_modes
        )

    @tag(ApiTaskTag.RANDOM_ALL)
    @task
    def random_all(self):
        """Test with random all."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE,
            ApiWeatherGroupMode.BY_OUTPUT_TYPE,
            ApiWeatherGroupMode.BY_ATTRIBUTE_LENGTH,
            ApiWeatherGroupMode.BY_DATE_COUNT
        ]
        dates1 = self.get_random_date_range()
        # test with random variables
        self.api.weather(
            self.product_type,
            self.get_random_output_type(),
            self.get_random_attributes(),
            dates1[0],
            dates1[1],
            lat=self.point[0],
            lon=self.point[1],
            group_modes=group_modes
        )
