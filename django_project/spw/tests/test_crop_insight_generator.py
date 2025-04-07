# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""

import csv
from datetime import date, timedelta, datetime
from unittest.mock import patch
import pytz

from django.contrib.gis.geos import Point
from django.test import TestCase

from core.factories import UserF
from core.group_email_receiver import _group_crop_plan_receiver
from gap.factories.crop_insight import CropInsightRequestFactory
from gap.factories.farm import FarmFactory, FarmGroupFactory
from gap.factories.grid import GridFactory
from gap.models.crop_insight import (
    FarmSuitablePlantingWindowSignal, FarmGroupIsNotSetException,
    FarmShortTermForecast
)
from gap.models.pest import Pest
from gap.models.preferences import Preferences
from gap.models.farm_group import FarmGroupCropInsightField
from gap.tasks.crop_insight import (
    generate_insight_report
)
from spw.factories import RModelFactory
from spw.generator.crop_insight import CropInsightFarmGenerator
from spw.tasks import clean_duplicate_farm_short_term_forecast

ltn_returns = {
    '07-20': {
        'date': '2023-07-20',
        'evapotranspirationSum': 10,
        'rainAccumulationSum': 5,
        'LTNPET': 8,
        'LTNPrecip': 3
    }
}


def always_retrying(
    self
):
    """Mock the function that always retry."""
    raise Exception('Test exception')


retry = {
    'count': 1
}


def retrying_2_times(
        self, historical_dict: dict
):
    """Mock the function just retry 2 times."""
    if retry['count'] <= 2:
        retry['count'] += 1
        raise Exception('Test exception')
    else:
        for k, v in historical_dict.items():
            historical_dict[k]['LTNPET'] = 8
            historical_dict[k]['LTNPrecip'] = 3
        return historical_dict


def create_ltn_data(historical_dict: dict):
    """Mock LTN Data."""
    for k, v in historical_dict.items():
        historical_dict[k]['LTNPET'] = 8
        historical_dict[k]['LTNPrecip'] = 3
    return historical_dict


class TestCropInsightGenerator(TestCase):
    """Unit test for Crop Insight Generator class."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '9.rainfall_classification.json',
        '10.pest.json',
        '1.spw_output.json'
    ]
    csv_headers = [
        'farmID', 'phoneNumber', 'latitude', 'longitude', 'SPWTopMessage',
        'SPWDescription',
        'TooWet',
        'last_4_days_mm',
        'last_2_days_mm',
        'today_tomorrow_mm',
        'day1_mm', 'day1_Chance', 'day1_Type',
        'day2_mm', 'day2_Chance', 'day2_Type',
        'day3_mm', 'day3_Chance', 'day3_Type',
        'day4_mm', 'day4_Chance', 'day4_Type',
        'day5_mm', 'day5_Chance', 'day5_Type',
        'day6_mm', 'day6_Chance', 'day6_Type',
        'day7_mm', 'day7_Chance', 'day7_Type',
        'day8_mm', 'day8_Chance', 'day8_Type',
        'day9_mm', 'day9_Chance', 'day9_Type',
        'day10_mm', 'day10_Chance', 'day10_Type',
        'day11_mm', 'day11_Chance', 'day11_Type',
        'day12_mm', 'day12_Chance', 'day12_Type',
        'day13_mm', 'day13_Chance', 'day13_Type',
        'prise_bean_fly_1', 'prise_bean_fly_2', 'prise_bean_fly_3',
        'prise_bean_fly_4', 'prise_bean_fly_5'
    ]

    def setUp(self):
        """Set the test class."""
        grid_1 = GridFactory()
        grid_2 = GridFactory()
        grid_3 = GridFactory()
        self.farm = FarmFactory.create(
            geometry=Point(11.1111111, 10.111111111),
            grid=grid_1,
        )
        self.farm_2 = FarmFactory.create(
            geometry=Point(22.22222222, 100.111111111),
            grid=grid_2
        )
        self.farm_3 = FarmFactory.create(
            geometry=Point(50.22222222, 50.111111111),
            grid=grid_3,
        )
        self.farm_4 = FarmFactory.create(
            geometry=Point(22.22222222, 100),
            grid=grid_2,
        )
        self.farm_5 = FarmFactory.create(
            geometry=Point(50.22222222, 50),
            grid=grid_3,
        )
        self.farm_group = FarmGroupFactory()
        self.farm_group.farms.add(
            self.farm, self.farm_2, self.farm_3, self.farm_4, self.farm_5
        )
        self.r_model = RModelFactory.create(name='test')
        self.today = datetime(
            2023, 7, 18, 0, 0, 0, tzinfo=pytz.UTC
        ).date()
        self.superuser = UserF.create(
            is_staff=True,
            is_superuser=True,
            is_active=True
        )

        group = _group_crop_plan_receiver()
        self.user_1 = UserF(email='user_1@email.com')
        self.user_2 = UserF(email='user_2@email.com')
        self.user_3 = UserF(email='user_3@email.com')
        self.user_4 = UserF(email='')
        self.farm_group.users.add(self.user_1, self.user_2, self.user_3)
        group.user_set.add(self.user_1, self.user_2)
        self.preferences = Preferences().load()
        self.preferences.crop_plan_config = {
            'lat_lon_decimal_digits': 4
        }
        self.preferences.save()

        # update prise headers
        self.pest = Pest.objects.get(name='Beanfly')
        FarmGroupCropInsightField.objects.filter(
            farm_group=self.farm_group,
            field__startswith=f'prise_{self.pest.short_name}_'
        ).update(active=True)

    @patch('gap.models.crop_insight.timezone')
    @patch('spw.generator.crop_insight.timezone')
    @patch('spw.generator.main.datetime')
    @patch('prise.generator.generate_prise_message')
    @patch('spw.generator.main.execute_spw_model')
    @patch('spw.generator.gap_input.GapInput._fetch_timelines_data')
    @patch('spw.generator.gap_input.GapInput._fetch_ltn_data')
    def test_spw_generator(
            self, mock_fetch_ltn_data, mock_fetch_timelines_data,
            mock_execute_spw_model, mock_prise, mock_now, mock_timezone,
            mock_timezone_2
    ):
        """Test calculate_from_point function."""
        mock_dt = datetime(
            2023, 7, 18, 0, 0, 0, tzinfo=pytz.UTC
        )
        mock_now.now.return_value = mock_dt
        mock_timezone.now.return_value = mock_dt
        mock_timezone_2.now.return_value = mock_dt
        last_day = self.today + timedelta(days=12)

        def create_timeline_data(
                output, _date, evap, rain, max, min, prec_prob
        ):
            """Create time data."""
            if last_day == _date:
                return

            output[_date.strftime('%m-%d')] = {
                'date': _date.strftime('%Y-%m-%d'),
                'evapotranspirationSum': evap,
                'rainAccumulationSum': rain,
                'temperatureMax': max,
                'precipitationProbability': prec_prob,
                'temperatureMin': min
            }
            create_timeline_data(
                output, _date + timedelta(days=1),
                evap, rain, max, min, prec_prob
            )

        # For farm 1
        mock_execute_spw_model.return_value = (
            True, {
                'metadata': {
                    'test': 'abcdef'
                },
                'goNoGo': ['Plant NOW Tier 1b'],
                'nearDaysLTNPercent': [10.0],
                'nearDaysCurPercent': [60.0],
                'tooWet': ['Likely too wet to plant'],
                'last4Days': [80],
                'last2Days': [60],
                'todayTomorrow': [40],
            }
        )
        fetch_timelines_data_val = {}
        create_timeline_data(
            fetch_timelines_data_val, self.today - timedelta(days=6),
            10, 10, 100, 0, 50
        )
        mock_fetch_timelines_data.return_value = fetch_timelines_data_val
        generator = CropInsightFarmGenerator(self.farm)

        # ------------------------------------------------------------
        # Check if _fetch_ltn_data always returns exception
        with (
            patch(
                'spw.generator.gap_input.GapInput._fetch_ltn_data',
                always_retrying
            )
        ):
            with self.assertRaises(Exception):
                generator.generate_spw()

        # ------------------------------------------------------------
        # Check if _fetch_ltn_data always returns exception
        with (
            patch(
                'spw.generator.gap_input.GapInput._fetch_ltn_data',
                retrying_2_times
            )
        ):
            generator.generate_spw()
        # ------------------------------------------------------------

        last_spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm
        ).last()
        forecast = self.farm.farmshorttermforecast_set.all().first()
        self.assertEqual(last_spw.signal, 'Plant NOW Tier 1b')
        self.assertEqual(self.farm.farmshorttermforecast_set.count(), 1)
        self.assertEqual(
            forecast.farmshorttermforecastdata_set.count(), 55
        )

        # For farm 2
        mock_fetch_ltn_data.return_value = create_ltn_data(
            fetch_timelines_data_val
        )
        mock_execute_spw_model.return_value = (
            True, {
                'metadata': {
                    'test': 'abcdef'
                },
                'goNoGo': ['Do NOT plant, DRY Tier 4b'],
                'nearDaysLTNPercent': [10.0],
                'nearDaysCurPercent': [60.0],
                'tooWet': ['Too wet to plant'],
                'last4Days': [100],
                'last2Days': [80],
                'todayTomorrow': [80],
            }
        )
        fetch_timelines_data_val = {}
        create_timeline_data(
            fetch_timelines_data_val, self.today - timedelta(days=10),
            1, 0.5, 4, 3, 10
        )
        mock_fetch_timelines_data.return_value = fetch_timelines_data_val
        generator = CropInsightFarmGenerator(self.farm_2)
        generator.generate_spw()
        last_spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm_2
        ).last()
        forecast = self.farm_2.farmshorttermforecast_set.all().first()
        self.assertEqual(last_spw.signal, 'Do NOT plant, DRY Tier 4b')
        self.assertEqual(
            self.farm_2.farmshorttermforecast_set.count(), 1
        )
        self.assertEqual(
            forecast.farmshorttermforecastdata_set.count(), 55
        )
        # For farm 3
        mock_fetch_ltn_data.return_value = {}
        mock_execute_spw_model.return_value = (
            True, {
                'metadata': {
                    'test': 'abcdef'
                },
                'goNoGo': '',
                'nearDaysLTNPercent': [10.0],
                'nearDaysCurPercent': [60.0],
                'tooWet': '',
                'last4Days': '',
                'last2Days': '',
                'todayTomorrow': '',
            }
        )
        mock_fetch_timelines_data.return_value = {}

        # Farm group is required, raise error
        with self.assertRaises(FarmGroupIsNotSetException):
            request = CropInsightRequestFactory.create(
                requested_at=mock_dt
            )
            generate_insight_report(request.id)

        # We mock the send email to get the attachments
        attachments = []

        def mock_send_fn(self, fail_silently=False):
            """Mock send messages."""
            for attachment in self.attachments:
                attachments.append(attachment)
            return 0

        # mock prise message
        mock_prise.return_value = ['prise pet 1', 'prise pet 2']

        # Mock the send email
        with patch("django.core.mail.EmailMessage.send", mock_send_fn):
            # Crop insight report
            self.request = CropInsightRequestFactory.create(
                farm_group=self.farm_group,
                requested_at=mock_dt
            )
            generate_insight_report(self.request.id)
            self.request.refresh_from_db()

            # Check the if of farm group in the path
            print('filename ', self.request.file.name)
            self.assertTrue(f'{self.farm_group.id}/' in self.request.file.name)

            # Check the attachment on email
            self.assertEqual(attachments[0][0], self.request.filename)

            # Check the file content
            with self.request.file.open(mode='r') as csv_file:
                csv_reader = csv.reader(csv_file)
                row_num = 1
                for row in csv_reader:
                    # Header
                    if row_num == 1:
                        self.assertEqual(row, self.csv_headers)

                    # Farm 1
                    elif row_num == 2:
                        # Farm Unique ID
                        self.assertEqual(row[0], self.farm.unique_id)
                        # Phone Number
                        self.assertEqual(row[1], self.farm.phone_number)
                        self.assertEqual(row[2], '10.1111')  # Latitude
                        self.assertEqual(row[3], '11.1111')  # Longitude
                        self.assertEqual(row[4], 'Plant Now')
                        self.assertEqual(
                            row[5],
                            'Both current forecast '
                            'historical rains have good signal to plant.'
                        )
                        # Too wet column
                        self.assertEqual(
                            row[6], 'Likely too wet to plant'
                        )  # tooWet
                        self.assertEqual(row[7], '80.0')  # last4Days
                        self.assertEqual(row[8], '60.0')  # last2Days
                        self.assertEqual(row[9], '40.0')  # todayTomorrow

                        # First day forecast
                        self.assertEqual(row[10], '10.0')  # Precip (daily)
                        self.assertEqual(row[11], '50.0')  # Precip % chance
                        self.assertEqual(row[12], 'Light rain')  # Precip Type

                        # prise columns
                        self.assertEqual(
                            row[self.csv_headers.index('prise_bean_fly_1')],
                            'prise pet 1'
                        )
                        self.assertEqual(
                            row[self.csv_headers.index('prise_bean_fly_2')],
                            'prise pet 2'
                        )
                        self.assertEqual(
                            row[self.csv_headers.index('prise_bean_fly_3')],
                            ''
                        )
                        self.assertEqual(
                            row[self.csv_headers.index('prise_bean_fly_4')],
                            ''
                        )
                        self.assertEqual(
                            row[self.csv_headers.index('prise_bean_fly_5')],
                            ''
                        )

                    # Farm 2
                    elif row_num == 3:
                        # Farm Unique ID
                        self.assertEqual(row[0], self.farm_2.unique_id)
                        # Phone Number
                        self.assertEqual(row[1], self.farm_2.phone_number)
                        self.assertEqual(row[2], '100.1111')  # Latitude
                        self.assertEqual(row[3], '22.2222')  # Longitude
                        self.assertEqual(row[4], 'DO NOT PLANT')
                        self.assertEqual(
                            row[5], 'Wait for more positive forecast.'
                        )
                        # Too wet column
                        self.assertEqual(row[6], 'Too wet to plant')  # tooWet
                        self.assertEqual(row[7], '100.0')  # last4Days
                        self.assertEqual(row[8], '80.0')  # last2Days
                        self.assertEqual(row[9], '80.0')  # todayTomorrow

                        # First day forecast
                        self.assertEqual(row[10], '10.0')  # Precip (daily)
                        self.assertEqual(row[11], '50.0')  # Precip % chance
                        self.assertEqual(row[12], 'Light rain')  # Precip Type

                    # Farm 3
                    elif row_num == 4:
                        # Farm Unique ID
                        self.assertEqual(row[0], self.farm_3.unique_id)
                        # Phone Number
                        self.assertEqual(row[1], self.farm_3.phone_number)
                        self.assertEqual(row[2], '50.1111')  # Latitude
                        self.assertEqual(row[3], '50.2222')  # Longitude
                        self.assertEqual(row[4], '')
                        self.assertEqual(row[5], '')

                        # Too wet column
                        self.assertEqual(row[6], '')  # tooWet
                        self.assertEqual(row[7], '')  # last4Days
                        self.assertEqual(row[8], '')  # last2Days
                        self.assertEqual(row[9], '')  # todayTomorrow

                        # First day forecast
                        self.assertEqual(row[10], '')  # Precip (daily)
                        self.assertEqual(row[11], '')  # Precip % chance
                        self.assertEqual(row[12], '')  # Precip Type

                    # Farm 4 has same grid with farm 2
                    elif row_num == 5:
                        # Farm Unique ID
                        self.assertEqual(row[0], self.farm_4.unique_id)
                        # Phone Number
                        self.assertEqual(row[1], self.farm_4.phone_number)
                        self.assertEqual(row[2], '100.0')  # Latitude
                        self.assertEqual(row[3], '22.2222')  # Longitude
                        self.assertEqual(row[4], 'DO NOT PLANT')
                        self.assertEqual(
                            row[5], 'Wait for more positive forecast.'
                        )
                        # Too wet column
                        self.assertEqual(row[6], 'Too wet to plant')  # tooWet
                        self.assertEqual(row[7], '100.0')  # last4Days
                        self.assertEqual(row[8], '80.0')  # last2Days
                        self.assertEqual(row[9], '80.0')  # todayTomorrow

                        # First day forecast
                        self.assertEqual(row[10], '10.0')  # Precip (daily)
                        self.assertEqual(row[11], '50.0')  # Precip % chance
                        self.assertEqual(row[12], 'Light rain')  # Precip Type

                    # Farm 5 has same grid with farm 3
                    elif row_num == 6:
                        # Farm Unique ID
                        self.assertEqual(row[0], self.farm_5.unique_id)
                        # Phone Number
                        self.assertEqual(row[1], self.farm_5.phone_number)
                        self.assertEqual(row[2], '50.0')  # Latitude
                        self.assertEqual(row[3], '50.2222')  # Longitude

                        # Too wet column
                        self.assertEqual(row[6], '')  # tooWet
                        self.assertEqual(row[7], '')  # last4Days
                        self.assertEqual(row[8], '')  # last2Days
                        self.assertEqual(row[9], '')  # todayTomorrow

                        # First day forecast
                        self.assertEqual(row[10], '')  # Precip (daily)
                        self.assertEqual(row[11], '')  # Precip % chance
                        self.assertEqual(row[12], '')  # Precip Type
                    row_num += 1

    def test_email_send(self):
        """Test email send when report created."""
        self.recipients = []
        parent = self

        def mock_send_fn(self, fail_silently=False):
            """Mock send messages."""
            parent.recipients = self.recipients()
            return 0

        with patch(
                "django.core.mail.EmailMessage.send", mock_send_fn
        ):
            FarmGroupFactory()
            used_group = FarmGroupFactory()
            used_group.users.add(parent.user_1, parent.user_2, parent.user_4)
            request = CropInsightRequestFactory.create(
                farm_group=used_group
            )
            request.run()
            self.recipients.sort()
            parent.assertEqual(len(self.recipients), 2)
            parent.assertEqual(
                self.recipients,
                [parent.user_1.email, parent.user_2.email]
            )

    def test_clean_duplicate_farm_short_term_forecast(self):
        """Test clean_duplicate_farm_short_term_forecast task."""
        # create FarmShortTermForecast
        FarmShortTermForecast.objects.create(
            farm=self.farm,
            forecast_date=date.today()
        )
        FarmShortTermForecast.objects.create(
            farm=self.farm,
            forecast_date=date.today()
        )
        FarmShortTermForecast.objects.create(
            farm=self.farm_2,
            forecast_date=date.today()
        )

        # Call the task
        clean_duplicate_farm_short_term_forecast()

        # Check that only one forecast per farm and date remains
        self.assertEqual(
            FarmShortTermForecast.objects.filter(
                farm=self.farm,
                forecast_date=date.today()
            ).count(),
            1
        )
        self.assertEqual(
            FarmShortTermForecast.objects.filter(
                farm=self.farm_2,
                forecast_date=date.today()
            ).count(),
            1
        )
