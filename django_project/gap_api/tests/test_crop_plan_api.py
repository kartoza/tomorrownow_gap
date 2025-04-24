# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Crop Plan.
"""

from django.contrib.gis.geos import Point
from django.urls import reverse

from core.tests.common import BaseAPIViewTest
from gap.factories.farm import FarmFactory
from gap.factories.main import AttributeFactory
from gap.models import (
    DatasetAttribute, FarmShortTermForecast, FarmShortTermForecastData
)
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset
from gap_api.api_views.crop_insight import CropPlanAPI


class CropPlanAPITest(BaseAPIViewTest):
    """Historical api test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def create_forecast_data(self, forecast, attr, date, value):
        """Create forecast data."""
        FarmShortTermForecastData.objects.get_or_create(
            forecast=forecast,
            dataset_attribute=attr,
            value_date=date,
            value=value
        )

    def setUp(self):
        """Init test class."""
        super().setUp()

        ds_forecast = tomorrowio_shortterm_forecast_dataset()
        attr = AttributeFactory()
        self.farm = FarmFactory.create(
            unique_id='farm-1',
            geometry=Point(0, 1)
        )
        self.farm_2 = FarmFactory.create(
            unique_id='farm-2',
            geometry=Point(2, 3)
        )

        ds_1, _ = DatasetAttribute.objects.get_or_create(
            dataset=ds_forecast,
            attribute=attr,
            source='SOURCE_1',
            source_unit=attr.unit
        )
        ds_2, _ = DatasetAttribute.objects.get_or_create(
            dataset=ds_forecast,
            attribute=attr,
            source='SOURCE_2',
            source_unit=attr.unit
        )
        self.view = CropPlanAPI.as_view()

        # Construct data
        forecast_1, _ = FarmShortTermForecast.objects.get_or_create(
            farm=self.farm,
            forecast_date='2020-01-01'
        )
        forecast_2, _ = FarmShortTermForecast.objects.get_or_create(
            farm=self.farm,
            forecast_date='2021-01-01'
        )
        self.create_forecast_data(forecast_1, ds_1, '2020-01-02', 0)
        self.create_forecast_data(forecast_1, ds_1, '2020-01-03', 1)
        self.create_forecast_data(forecast_1, ds_2, '2020-01-02', 2)
        self.create_forecast_data(forecast_1, ds_2, '2020-01-03', 3)
        self.create_forecast_data(forecast_2, ds_1, '2020-01-02', 4)
        self.create_forecast_data(forecast_2, ds_1, '2020-01-03', 5)
        self.create_forecast_data(forecast_2, ds_2, '2020-01-02', 6)
        self.create_forecast_data(forecast_2, ds_2, '2020-01-03', 7)

    def request(self, url):
        """Request url."""
        request = self.factory.get(url)
        request.user = self.superuser
        return self.view(request)

    def test_no_login(self):
        """Test no login."""
        request = self.factory.get(
            reverse('api:v1:crop-plan')
        )
        response = self.view(request)
        self.assertEqual(response.status_code, 401)

    def _find_farms(self, farms, farm_id):
        """Find farm in list."""
        for farm in farms:
            if farm['farmID'] == farm_id:
                return farm
        return None

    def test_params_error(self):
        """Test format."""
        request = self.factory.get(
            reverse('api:v1:crop-plan') + '?format=test'
        )
        request.user = self.superuser
        response = self.view(request)
        self.assertEqual(response.status_code, 404)

        request = self.factory.get(
            reverse('api:v1:crop-plan') + '?generated_date=test'
        )
        request.user = self.superuser
        response = self.view(request)
        self.assertEqual(response.status_code, 400)

    def test_correct(self):
        """Test correct."""
        response = self.request(
            reverse('api:v1:crop-plan') +
            '?farm_ids=farm-1,farm-2'
        )
        self.assertEqual(response.status_code, 200)
        _data = response.data
        self.assertEqual(len(_data), 2)
        farm1 = self._find_farms(_data, 'farm-1')
        farm2 = self._find_farms(_data, 'farm-2')
        self.assertIsNotNone(farm1)
        self.assertIsNotNone(farm2)
        self.assertEqual(farm1['latitude'], 1)
        self.assertEqual(farm2['latitude'], 3)
        self.assertEqual(farm1['longitude'], 0)
        self.assertEqual(farm2['longitude'], 2)
        self.assertEqual(farm1['day1_SOURCE_1'], '')
        self.assertEqual(farm2['day1_SOURCE_1'], '')
        self.assertEqual(farm1['day1_SOURCE_2'], '')
        self.assertEqual(farm2['day1_SOURCE_2'], '')

    def test_correct_with_param(self):
        """Test correct."""
        response = self.request(
            reverse('api:v1:crop-plan') +
            '?farm_ids=farm-2&generated_date=2020-01-01&format=json'
        )
        self.assertEqual(
            response.accepted_media_type, 'application/json'
        )
        self.assertEqual(response.status_code, 200)
        _data = response.data
        self.assertEqual(len(_data), 1)
        self.assertEqual(_data[0]['farmID'], 'farm-2')
        self.assertEqual(_data[0]['latitude'], 3)
        self.assertEqual(_data[0]['longitude'], 2)
        self.assertEqual(_data[0]['day1_SOURCE_1'], '')
        self.assertEqual(_data[0]['day1_SOURCE_2'], '')

        response = self.request(
            reverse('api:v1:crop-plan') +
            '?farm_ids=farm-1&generated_date=2020-01-01'
        )
        self.assertEqual(response.status_code, 200)
        _data = response.data
        self.assertEqual(len(_data), 1)
        self.assertEqual(_data[0]['farmID'], 'farm-1')
        self.assertEqual(_data[0]['latitude'], 1)
        self.assertEqual(_data[0]['longitude'], 0)
        self.assertEqual(_data[0]['day1_SOURCE_1'], 0)
        self.assertEqual(_data[0]['day1_SOURCE_2'], 2)
        self.assertEqual(_data[0]['day2_SOURCE_1'], 1)
        self.assertEqual(_data[0]['day2_SOURCE_2'], 3)

        response = self.request(
            reverse('api:v1:crop-plan') +
            '?farm_ids=farm-1&generated_date=2021-01-01'
        )
        self.assertEqual(response.status_code, 200)
        _data = response.data
        self.assertEqual(len(_data), 1)
        self.assertEqual(_data[0]['farmID'], 'farm-1')
        self.assertEqual(_data[0]['latitude'], 1)
        self.assertEqual(_data[0]['longitude'], 0)
        self.assertEqual(_data[0]['day1_SOURCE_1'], 4)
        self.assertEqual(_data[0]['day1_SOURCE_2'], 6)
        self.assertEqual(_data[0]['day2_SOURCE_1'], 5)
        self.assertEqual(_data[0]['day2_SOURCE_2'], 7)

    def test_correct_with_csv(self):
        """Test correct."""
        response = self.request(
            reverse('api:v1:crop-plan') +
            '?farm_ids=farm-2&generated_date=2020-01-01&format=csv'
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.accepted_media_type, 'text/csv')
