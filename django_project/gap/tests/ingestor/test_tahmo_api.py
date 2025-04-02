# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Arable Ingestor.
"""
import os
from datetime import datetime, timezone
from unittest.mock import patch

import responses
from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import GEOSGeometry
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.ingestor.exceptions import EnvIsNotSetException
from gap.ingestor.tahmo_api import (
    TahmoAPIIngestor,
    TAHMO_API_USERNAME_ENV_NAME, TAHMO_API_PASSWORD_ENV_NAME
)
from gap.models import (
    Country, Station, IngestorSession, IngestorSessionStatus, IngestorType
)
from gap.models.preferences import Preferences
from gap.tests.mock_response import BaseTestWithPatchResponses, PatchRequest


class TahmoAPIIngestorTest(BaseTestWithPatchResponses, TestCase):
    """Tahmo ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.TAHMO_API
    responses_folder = absolute_path(
        'gap', 'tests', 'ingestor', 'data', 'tahmo'
    )

    @property
    def mock_requests(self):
        """Mock requests."""
        preferences = Preferences.load()
        ingestor = TahmoAPIIngestor(IngestorSession())
        patches = []
        for key, value in ingestor.attributes.items():
            if key == 'pr':
                continue
            for station_code in ['TD00001', 'TD00002']:
                patches.append(
                    PatchRequest(
                        (
                            f'{preferences.tahmo_api_url}'
                            f'/services/measurements/v2/'
                            f'stations/{station_code}/measurements/raw?'
                            f'start=2023-03-02T00%3A00%3A00Z&'
                            f'end=2024-03-01T00%3A00%3A00Z&variable={key}'
                        ),
                        response={}
                    )
                )
                patches.append(
                    PatchRequest(
                        (
                            f'{preferences.tahmo_api_url}'
                            f'/services/measurements/v2/'
                            f'stations/{station_code}/measurements/raw?'
                            f'start=2023-06-02T00%3A00%3A00Z&'
                            f'end=2024-06-01T00%3A00%3A00Z&variable={key}'
                        ),
                        response={}
                    )
                )

        return patches + [
            # Devices API
            PatchRequest(
                f'{preferences.tahmo_api_url}/services/assets/v2/stations',
                file_response=os.path.join(
                    self.responses_folder, 'devices.json'
                )
            ),
            PatchRequest(
                (
                    f'{preferences.tahmo_api_url}/services/measurements/v2/'
                    f'stations/TD00001/measurements/raw?'
                    f'start=2023-03-02T00%3A00%3A00Z&'
                    f'end=2024-03-01T00%3A00%3A00Z&variable=pr'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'TD00001.1.json'
                )
            ),
            PatchRequest(
                (
                    f'{preferences.tahmo_api_url}/services/measurements/v2/'
                    f'stations/TD00002/measurements/raw?'
                    f'start=2023-03-02T00%3A00%3A00Z&'
                    f'end=2024-03-01T00%3A00%3A00Z&variable=pr'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'TD00001.1.json'
                )
            ),
            PatchRequest(
                (
                    f'{preferences.tahmo_api_url}/services/measurements/v2/'
                    f'stations/TD00001/measurements/raw?'
                    f'start=2024-03-01T00%3A00%3A00Z&'
                    f'end=2024-06-01T00%3A00%3A00Z&variable=pr'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'TD00001.2.json'
                )
            ),
            PatchRequest(
                (
                    f'{preferences.tahmo_api_url}/services/measurements/v2/'
                    f'stations/TD00002/measurements/raw?'
                    f'start=2023-03-02T00%3A00%3A00Z&'
                    f'end=2024-03-01T00%3A00%3A00Z&variable=pr'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'TD00001.1.json'
                )
            ),
            PatchRequest(
                (
                    f'{preferences.tahmo_api_url}/services/measurements/v2/'
                    f'stations/TD00002/measurements/raw?'
                    f'start=2024-03-01T00%3A00%3A00Z&'
                    f'end=2024-06-01T00%3A00%3A00Z&variable=pr'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'TD00002.2.json'
                )
            ),
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
        os.environ[TAHMO_API_USERNAME_ENV_NAME] = ''
        os.environ[TAHMO_API_PASSWORD_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type,
            trigger_task=False
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(
            session.notes,
            EnvIsNotSetException(TAHMO_API_USERNAME_ENV_NAME).message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

        # No password
        os.environ[TAHMO_API_USERNAME_ENV_NAME] = 'Username'
        os.environ[TAHMO_API_PASSWORD_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type,
            trigger_task=False
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(
            session.notes,
            EnvIsNotSetException(TAHMO_API_PASSWORD_ENV_NAME).message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    @patch('gap.ingestor.tahmo_api.timezone')
    @responses.activate
    def test_run(self, mock_timezone):
        """Test run."""
        mock_timezone.now.return_value = datetime(
            2024, 3, 1, 0, 0, 0
        )
        mock_timezone.utc = timezone.utc

        self.init_mock_requests()
        os.environ[TAHMO_API_USERNAME_ENV_NAME] = 'Username'
        os.environ[TAHMO_API_PASSWORD_ENV_NAME] = 'password'
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False
        )
        session.run()
        session.refresh_from_db()
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)

        # Check station 1
        station = Station.objects.get(code='TD00001')
        self.assertEqual(station.name, 'Device 1')
        self.assertEqual(station.country.name, 'Kenya')
        self.assertEqual(station.station_type.name, 'Disdrometer')
        self.assertEqual(station.metadata['type'], 'Device')
        # check measurements
        self.assertEqual(station.measurement_set.count(), 3)
        self.assertEqual(
            TahmoAPIIngestor.last_date_time(
                station, station.measurement_set.first().dataset_attribute
            ),
            datetime(
                2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc
            )
        )

        # Check station 2
        station = Station.objects.get(code='TD00002')
        self.assertEqual(station.name, 'Device 2')
        self.assertEqual(station.country.name, 'Kenya')
        self.assertEqual(station.station_type.name, 'Disdrometer')
        self.assertEqual(station.metadata['type'], 'Device')
        # check measurements
        self.assertEqual(station.measurement_set.count(), 3)
        self.assertEqual(
            TahmoAPIIngestor.last_date_time(
                station, station.measurement_set.first().dataset_attribute
            ),
            datetime(
                2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc
            )
        )

        # We run second time now move to next 3 months
        mock_timezone.now.return_value = datetime(
            2024, 6, 1, 0, 0, 0
        )
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)

        # check measurements 1
        station = Station.objects.get(code='TD00001')
        self.assertEqual(station.measurement_set.count(), 4)
        self.assertEqual(
            TahmoAPIIngestor.last_date_time(
                station, station.measurement_set.first().dataset_attribute
            ),
            datetime(
                2024, 4, 1, 0, 0, 0, tzinfo=timezone.utc
            )
        )
        # check measurements 2
        station = Station.objects.get(code='TD00002')
        self.assertEqual(station.measurement_set.count(), 4)
        self.assertEqual(
            TahmoAPIIngestor.last_date_time(
                station, station.measurement_set.first().dataset_attribute
            ),
            datetime(
                2024, 4, 1, 0, 0, 0, tzinfo=timezone.utc
            )
        )
