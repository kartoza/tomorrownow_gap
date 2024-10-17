# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Windborne Systems Ingestor.
"""
import os

import responses
from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import GEOSGeometry, Point
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.ingestor.exceptions import EnvIsNotSetException
from gap.ingestor.wind_borne_systems import (
    WindBorneSystemsAPI, USERNAME_ENV_NAME, PASSWORD_ENV_NAME, PROVIDER,
    STATION_TYPE
)
from gap.models import (
    Provider, StationType, Country, Station, IngestorSession,
    IngestorSessionStatus, IngestorType
)
from gap.tests.mock_response import BaseTestWithPatchResponses, PatchRequest


class WindBorneSystemsAPIIngestorTest(BaseTestWithPatchResponses, TestCase):
    """WindBorneSystems ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.WIND_BORNE_SYSTEMS_API
    responses_folder = absolute_path(
        'gap', 'tests', 'ingestor', 'data', 'windbornesystems'
    )

    @property
    def mock_requests(self):
        """Mock requests."""
        base_url = WindBorneSystemsAPI.base_url
        return [
            PatchRequest(
                f'{base_url}/missions.json?',
                file_response=os.path.join(
                    self.responses_folder, 'missions.json'
                )
            ),
            PatchRequest(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'mission_id=mission-1'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'mission_1.since_1.json'
                )
            ),
            PatchRequest(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'mission_id=mission-1&'
                    f'since=1727308800'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'mission_1.since_2.json'
                )
            ),
            PatchRequest(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'mission_id=mission-1&'
                    f'since=1727395200'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'mission_1.since_3.json'
                )
            ),
            PatchRequest(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'mission_id=mission-2'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'mission_2.since_1.json'
                )
            ),
            PatchRequest(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'mission_id=mission-2&'
                    f'since=1727308800'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'mission_2.since_2.json'
                )
            ),
            PatchRequest(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'mission_id=mission-2&'
                    f'since=1727395200'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'mission_2.since_3.json'
                )
            )
        ]

    def setUp(self):
        """Init test case."""
        # Init kenya Country
        shp_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'Kenya.geojson'
        )
        data_source = DataSource(shp_path)
        layer = data_source[0]
        for feature in layer:
            geometry = GEOSGeometry(feature.geom.wkt, srid=4326)
            Country.objects.create(
                name=feature['name'],
                iso_a3=feature['iso_a3'],
                geometry=geometry
            )

    def test_no_username_and_password(self):
        """Test no API Key."""
        # No username
        os.environ[USERNAME_ENV_NAME] = ''
        os.environ[PASSWORD_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(
            session.notes,
            EnvIsNotSetException(USERNAME_ENV_NAME).message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

        # No password
        os.environ[USERNAME_ENV_NAME] = 'Username'
        os.environ[PASSWORD_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(
            session.notes,
            EnvIsNotSetException(PASSWORD_ENV_NAME).message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    @responses.activate
    def test_run(self):
        """Test run."""
        self.init_mock_requests()
        os.environ[USERNAME_ENV_NAME] = 'Username'
        os.environ[PASSWORD_ENV_NAME] = 'password'

        # Create mission 2
        point = Point(
            x=36.756561,
            y=-1.131241,
            srid=4326
        )
        provider = Provider.objects.get(
            name=PROVIDER
        )
        station_type = StationType.objects.get(
            name=STATION_TYPE
        )
        Station.objects.update_or_create(
            provider=provider,
            station_type=station_type,
            code='mission-2',
            defaults={
                'name': 'mission-2',
                'geometry': point,
                'altitude': 500,
            }
        )

        # First import
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        first_station = Station.objects.first()
        self.assertEqual(
            first_station.stationhistory_set.count(), 2
        )

        # Next import
        session.run()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)

        # First station
        station = Station.objects.get(code='mission-1')
        self.assertEqual(
            station.stationhistory_set.count(), 3
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='pressure'
                ).values_list('value', flat=True)
            ),
            [10, 20, 30]
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='humidity'
                ).values_list('value', flat=True)
            ),
            [1, 2, 3]
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='specific_humidity'
                ).values_list('value', flat=True)
            ),
            [2, 3, 4]
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='temperature'
                ).values_list('value', flat=True)
            ),
            [20, 30, 40]
        )
        self.assertEqual(station.altitude, 30)
        self.assertEqual(
            list(
                station.stationhistory_set.values_list(
                    'altitude', flat=True
                )
            ),
            [10, 20, 30]
        )

        # Second station
        station = Station.objects.get(code='mission-2')
        self.assertEqual(
            station.stationhistory_set.count(), 3
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='pressure'
                ).values_list('value', flat=True)
            ),
            [200, 300, 400]
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='humidity'
                ).values_list('value', flat=True)
            ),
            [1, 2, 3]
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='specific_humidity'
                ).values_list('value', flat=True)
            ),
            [10, 20, 30]
        )
        self.assertEqual(
            list(
                station.measurement_set.filter(
                    dataset_attribute__source='temperature'
                ).values_list('value', flat=True)
            ),
            [10, 20, 30]
        )
        self.assertEqual(station.altitude, 700)
        self.assertEqual(
            list(
                station.stationhistory_set.values_list(
                    'altitude', flat=True
                )
            ),
            [500, 600, 700]
        )
