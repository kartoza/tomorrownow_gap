# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tio Collector.
"""
import json
import os
import zipfile
from datetime import datetime
from unittest.mock import patch

import responses
from django.conf import settings
from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import Polygon, GEOSGeometry
from django.core.files.storage import default_storage
from django.test import TestCase
from django.utils import timezone

from core.settings.utils import absolute_path
from gap.factories.grid import GridFactory
from gap.ingestor.tio_shortterm import path
from gap.models import (
    Country, IngestorSessionStatus, IngestorType
)
from gap.models.dataset import DataSourceFile
from gap.models.ingestor import CollectorSession
from gap.tests.mock_response import BaseTestWithPatchResponses, PatchRequest


class TioShortTermCollectorTest(BaseTestWithPatchResponses, TestCase):
    """Tio Collector test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.TIO_FORECAST_COLLECTOR
    responses_folder = absolute_path(
        'gap', 'tests', 'ingestor', 'data', 'tio_shorterm_collector'
    )
    api_key = 'tomorrow_api_key'

    def setUp(self):
        """Init test case."""
        os.environ['TOMORROW_IO_API_KEY'] = self.api_key
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

    @property
    def mock_requests(self):
        """Mock requests."""
        return [
            # Devices API
            PatchRequest(
                f'https://api.tomorrow.io/v4/timelines?apikey={self.api_key}',
                file_response=os.path.join(
                    self.responses_folder, 'test.json'
                ),
                request_method='POST'
            )
        ]

    def test_path(self):
        """Test path."""
        self.assertEqual(
            path('test'),
            f'{settings.STORAGE_DIR_PREFIX}tio-short-term-collector/test'
        )

    def test_collector_empty_grid(self):
        """Testing collector."""
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(DataSourceFile.objects.count(), 1)
        _file = default_storage.open(DataSourceFile.objects.first().name)
        with zipfile.ZipFile(_file, 'r') as zip_file:
            self.assertEqual(len(zip_file.filelist), 0)

    @patch('gap.ingestor.tio_shortterm.timezone')
    @responses.activate
    def test_collector_one_grid(self, mock_timezone):
        """Testing collector."""
        self.init_mock_requests()
        today = datetime(
            2024, 10, 1, 6, 0, 0
        )
        today = timezone.make_aware(
            today, timezone.get_default_timezone()
        )
        mock_timezone.now.return_value = today
        grid = GridFactory(
            geometry=Polygon(
                (
                    (0, 0), (0, 0.01), (0.01, 0.01), (0.01, 0), (0, 0)
                )
            )
        )
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(DataSourceFile.objects.count(), 1)
        data_source = DataSourceFile.objects.first()
        self.assertIn('forecast_date', data_source.metadata)
        self.assertEqual(
            data_source.metadata['forecast_date'], today.date().isoformat())
        _file = default_storage.open(data_source.name)
        with zipfile.ZipFile(_file, 'r') as zip_file:
            self.assertEqual(len(zip_file.filelist), 1)
            _file = zip_file.open(f'grid-{grid.id}.json')
            _data = json.loads(_file.read().decode('utf-8'))
            print(_data)
            self.assertEqual(
                _data,
                {
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [0.005, 0.005]
                    },
                    'data': [
                        {
                            'datetime': '2024-10-01T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 24.38,
                                'precipitation_probability': 0,
                                'humidity_maximum': 84,
                                'humidity_minimum': 69,
                                'wind_speed_avg': 4.77,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-02T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 80,
                                'humidity_minimum': 71,
                                'wind_speed_avg': 4.35,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-03T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 79,
                                'humidity_minimum': 73,
                                'wind_speed_avg': 5.58,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-04T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 78,
                                'humidity_minimum': 72,
                                'wind_speed_avg': 5.74,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-05T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 76,
                                'humidity_minimum': 70,
                                'wind_speed_avg': 5.09,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-06T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 76,
                                'humidity_minimum': 72,
                                'wind_speed_avg': 4.01,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-07T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28.5,
                                'min_temperature': 27,
                                'precipitation_probability': 0,
                                'humidity_maximum': 76,
                                'humidity_minimum': 70,
                                'wind_speed_avg': 3.82,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-08T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27.5,
                                'precipitation_probability': 5,
                                'humidity_maximum': 78,
                                'humidity_minimum': 72,
                                'wind_speed_avg': 4.12,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-09T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27.5,
                                'precipitation_probability': 5,
                                'humidity_maximum': 80,
                                'humidity_minimum': 74,
                                'wind_speed_avg': 5.29,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-10T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27.5,
                                'precipitation_probability': 5,
                                'humidity_maximum': 80,
                                'humidity_minimum': 73,
                                'wind_speed_avg': 4.96,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-11T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 29,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 77,
                                'humidity_minimum': 68,
                                'wind_speed_avg': 4.1,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-12T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28.5,
                                'min_temperature': 27,
                                'precipitation_probability': 5,
                                'humidity_maximum': 78,
                                'humidity_minimum': 70,
                                'wind_speed_avg': 4.42,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-13T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 27.5,
                                'precipitation_probability': 5,
                                'humidity_maximum': 78,
                                'humidity_minimum': 72,
                                'wind_speed_avg': 4.52,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            'datetime': '2024-10-14T06:00:00+00:00',
                            'values': {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 28,
                                'min_temperature': 24.12,
                                'precipitation_probability': 5,
                                'humidity_maximum': 78,
                                'humidity_minimum': 72,
                                'wind_speed_avg': 4.74,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        },
                        {
                            "datetime": "2024-10-15T06:00:00+00:00",
                            "values": {
                                'total_rainfall': 0,
                                'total_evapotranspiration_flux': None,
                                'max_temperature': 24.9,
                                'min_temperature': 24.12,
                                'precipitation_probability': 5,
                                'humidity_maximum': 77.83,
                                'humidity_minimum': 72.77,
                                'wind_speed_avg': 3.17,
                                'solar_radiation': None,
                                'weather_code': None,
                                'flood_index': None
                            }
                        }
                    ]
                }

            )
