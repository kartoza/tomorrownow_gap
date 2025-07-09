# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tio Collector.
"""
import json
import os
import uuid
import zipfile
import tempfile
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from unittest.mock import patch, AsyncMock

import responses

from django.conf import settings
from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import Polygon, GEOSGeometry
from django.core.files.storage import default_storage, storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from functools import partial

from core.settings.utils import absolute_path
from gap.factories.grid import GridFactory
from gap.ingestor.tomorrowio import (
    path, TioShortTermCollector, TioShortTermDuckDBCollector,
    TioShortTermHourlyDuckDBCollector, TioShortTermDailyCollector,
    TioShortTermHourlyCollector
)
from gap.models import (
    Country, IngestorSessionStatus, IngestorType,
    Dataset, DatasetStore, DataSourceFile
)
from gap.models.ingestor import CollectorSession
from gap.tests.mock_response import BaseTestWithPatchResponses, PatchRequest


def mock_collector_run(self_obj, cls, working_dir):
    """Mock collector run."""
    # Mock the collector run method
    collector = cls(
        self_obj, working_dir=working_dir
    )
    collector.run()


def mock_collector_run_with_dt(self_obj, dt, cls, working_dir):
    """Mock collector run."""
    # Mock the collector run method
    collector = cls(
        self_obj, working_dir=working_dir
    )
    collector.forecast_date = dt
    collector.start_dt = dt - timedelta(days=6)
    collector.end_dt = dt + timedelta(days=15)
    collector.run()


class DailyDuckDBAssert:
    """Assert daily DuckDB."""

    def assert_duckdb_file(self, data_source: DataSourceFile):
        """Download and check duckdb file."""
        s3_storage: S3Boto3Storage = storages["gap_products"]
        remote_path = data_source.metadata['remote_url']
        with tempfile.NamedTemporaryFile(suffix='.duckdb') as tmp:
            # Download the file
            with (
                s3_storage.open(remote_path, "rb") as remote_file,
                open(tmp.name, "wb") as local_file
            ):
                local_file.write(remote_file.read())
            duckdb_conn = duckdb.connect(tmp.name)
            # Check the number of tables in the database
            tables = duckdb_conn.execute("SHOW TABLES").fetchall()
            self.assertEqual(len(tables), 1)
            # Check the table name
            self.assertEqual(tables[0][0], "weather")
            # Check the number of rows in the table
            rows = duckdb_conn.execute(
                "SELECT COUNT(*) FROM weather"
            ).fetchone()
            self.assertEqual(rows[0], 15)
            # Check the columns in the table
            columns = duckdb_conn.execute("DESCRIBE weather").fetchall()
            self.assertEqual(len(columns), 18)
            columns_str = [col[0] for col in columns]
            self.assertIn('id', columns_str)
            self.assertIn('grid_id', columns_str)
            self.assertIn('lat', columns_str)
            self.assertIn('lon', columns_str)
            self.assertIn('date', columns_str)
            self.assertIn('time', columns_str)
            self.assertIn('total_rainfall', columns_str)
            self.assertIn('total_evapotranspiration_flux', columns_str)
            self.assertIn('max_temperature', columns_str)
            self.assertIn('min_temperature', columns_str)
            self.assertIn('precipitation_probability', columns_str)
            self.assertIn('humidity_maximum', columns_str)
            self.assertIn('humidity_minimum', columns_str)
            self.assertIn('wind_speed_avg', columns_str)
            self.assertIn('solar_radiation', columns_str)
            self.assertIn('weather_code', columns_str)
            self.assertIn('flood_index', columns_str)
            self.assertIn('wind_direction', columns_str)
            # Check the data in the table
            data = duckdb_conn.sql(
                "SELECT * FROM weather where date='2024-10-15'"
            ).to_df()
            self.assertEqual(data.shape[0], 1)
            data = data.drop(
                columns=['id', 'grid_id', 'lat', 'lon', 'date', 'time']
            )
            # print(data.iloc[0].to_dict())
            # compare dataframe
            pd.testing.assert_frame_equal(
                data,
                pd.DataFrame([
                    {
                        'solar_radiation': np.nan,
                        'total_evapotranspiration_flux': np.nan,
                        'max_temperature': 24.9,
                        'total_rainfall': 0.0,
                        'min_temperature': 24.12,
                        'precipitation_probability': 5.0,
                        'humidity_maximum': 77.83,
                        'humidity_minimum': 72.77,
                        'wind_speed_avg': 3.17,
                        'weather_code': np.nan,
                        'flood_index': np.nan,
                        'wind_direction': np.nan
                    }
                ])
            )
            # Close the connection
            duckdb_conn.close()

        # remove remote file
        s3_storage.delete(remote_path)


class HourlyDuckDBAssert:
    """Assert hourly DuckDB."""

    def assert_duckdb_file(self, data_source: DataSourceFile):
        """Download and check duckdb file."""
        s3_storage: S3Boto3Storage = storages["gap_products"]
        remote_path = data_source.metadata['remote_url']
        with tempfile.NamedTemporaryFile(suffix='.duckdb') as tmp:
            # Download the file
            with (
                s3_storage.open(remote_path, "rb") as remote_file,
                open(tmp.name, "wb") as local_file
            ):
                local_file.write(remote_file.read())
            duckdb_conn = duckdb.connect(tmp.name)
            # Check the number of tables in the database
            tables = duckdb_conn.execute("SHOW TABLES").fetchall()
            self.assertEqual(len(tables), 1)
            # Check the table name
            self.assertEqual(tables[0][0], "weather")
            # Check the number of rows in the table
            rows = duckdb_conn.execute(
                "SELECT COUNT(*) FROM weather"
            ).fetchone()
            self.assertEqual(rows[0], 24)
            # Check the columns in the table
            columns = duckdb_conn.execute("DESCRIBE weather").fetchall()
            self.assertEqual(len(columns), 16)
            columns_str = [col[0] for col in columns]
            self.assertIn('id', columns_str)
            self.assertIn('grid_id', columns_str)
            self.assertIn('lat', columns_str)
            self.assertIn('lon', columns_str)
            self.assertIn('date', columns_str)
            self.assertIn('time', columns_str)
            self.assertIn('total_rainfall', columns_str)
            self.assertIn('total_evapotranspiration_flux', columns_str)
            self.assertIn('temperature', columns_str)
            self.assertIn('precipitation_probability', columns_str)
            self.assertIn('humidity', columns_str)
            self.assertIn('wind_speed', columns_str)
            self.assertIn('solar_radiation', columns_str)
            self.assertIn('weather_code', columns_str)
            self.assertIn('flood_index', columns_str)
            # Check the data in the table
            data = duckdb_conn.sql(
                "SELECT * FROM weather where date='2025-04-24'"
            ).to_df()
            self.assertEqual(data.shape[0], 24)
            data = data[data['time'] == time(6, 0, 0)]
            data = data.drop(
                columns=['id', 'grid_id', 'lat', 'lon', 'date', 'time']
            )
            data = data.reset_index(drop=True)
            # print(data)
            # print(data.iloc[0].to_dict())
            # compare dataframe
            pd.testing.assert_frame_equal(
                data,
                pd.DataFrame([
                    {
                        'solar_radiation': 658.0,
                        'total_evapotranspiration_flux': 0.365,
                        'total_rainfall': 0.0,
                        'temperature': 21.7,
                        'wind_speed': 2.3,
                        'precipitation_probability': 0.0,
                        'weather_code': 1100.0,
                        'flood_index': np.nan,
                        'humidity': 61.0,
                        'wind_direction': np.nan
                    }
                ])
            )
            # Close the connection
            duckdb_conn.close()

        # remove remote file
        s3_storage.delete(remote_path)


class TioShortTermCollectorTest(BaseTestWithPatchResponses, TestCase):
    """Tio Collector test case."""

    fixtures = [
        '1.object_storage_manager.json',
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

    @patch.object(CollectorSession, '_run')
    def test_collector_empty_grid(self, mock_run):
        """Testing collector."""
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        mock_run.side_effect = partial(
            mock_collector_run,
            session,
            TioShortTermCollector
        )
        session.run()
        session.refresh_from_db()
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(DataSourceFile.objects.count(), 1)
        _file = default_storage.open(DataSourceFile.objects.first().name)
        with zipfile.ZipFile(_file, 'r') as zip_file:
            self.assertEqual(len(zip_file.filelist), 0)

    @patch.object(CollectorSession, '_run')
    @patch('gap.ingestor.tomorrowio.json_collector.timezone')
    @responses.activate
    def test_collector_one_grid(self, mock_timezone, mock_run):
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
        mock_run.side_effect = partial(
            mock_collector_run,
            session,
            TioShortTermCollector
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
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
                                'flood_index': None,
                                'wind_direction': None
                            }
                        }
                    ]
                }

            )


class TioShortTermDuckDBCollectorTest(
    BaseTestWithPatchResponses, DailyDuckDBAssert, TransactionTestCase
):
    """Tio DuckDB Collector test case."""

    fixtures = [
        '1.object_storage_manager.json',
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
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
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

    def assert_empty_duckdb_file(self, data_source: DataSourceFile):
        """Download and check duckdb file."""
        s3_storage: S3Boto3Storage = storages["gap_products"]
        with tempfile.NamedTemporaryFile(suffix='.duckdb') as tmp:
            remote_path = data_source.metadata['remote_url']
            # Download the file
            with (
                s3_storage.open(remote_path, "rb") as remote_file,
                open(tmp.name, "wb") as local_file
            ):
                local_file.write(remote_file.read())
            duckdb_conn = duckdb.connect(tmp.name)
            # Check the number of tables in the database
            tables = duckdb_conn.execute("SHOW TABLES").fetchall()
            self.assertEqual(len(tables), 1)
            # Check the table name
            self.assertEqual(tables[0][0], "weather")
            # Check the number of rows in the table
            rows = duckdb_conn.execute(
                "SELECT COUNT(*) FROM weather"
            ).fetchone()
            self.assertEqual(rows[0], 0)

    @patch.object(CollectorSession, '_run')
    @patch('gap.ingestor.tomorrowio.threads_collector.timezone')
    @responses.activate
    def test_collector_one_grid(self, mock_timezone, mock_run):
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
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1
            }
        )
        mock_run.side_effect = partial(
            mock_collector_run,
            session,
            TioShortTermDuckDBCollector
        )
        # create DataSourceFile for the session
        data_source = DataSourceFile.objects.create(
            name=f'{str(uuid.uuid4())}.duckdb',
            dataset=self.dataset,
            start_date_time=today,
            end_date_time=today,
            format=DatasetStore.DUCKDB,
            created_on=timezone.now(),
            metadata={
                'forecast_date': (
                    today.isoformat()
                ),
                'total_grid': 1,
                'start_grid_id': grid.id,
                'end_grid_id': grid.id,
            }
        )
        session.dataset_files.set([data_source])
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(session.dataset_files.count(), 1)
        data_source = session.dataset_files.first()
        self.assertIn('forecast_date', data_source.metadata)
        self.assertIn('remote_url', data_source.metadata)
        self.assert_duckdb_file(data_source)

    @patch.object(CollectorSession, '_run')
    @patch('gap.ingestor.tomorrowio.threads_collector.timezone')
    @responses.activate
    def test_collector_one_grid_start_new(self, mock_timezone, mock_run):
        """Testing collector."""
        self.init_mock_requests()
        today = datetime(
            2024, 10, 1, 6, 0, 0
        )
        today = timezone.make_aware(
            today, timezone.get_default_timezone()
        )
        mock_timezone.now.return_value = today
        GridFactory(
            geometry=Polygon(
                (
                    (0, 0), (0, 0.01), (0.01, 0.01), (0.01, 0), (0, 0)
                )
            )
        )
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1
            }
        )
        mock_run.side_effect = partial(
            mock_collector_run,
            session,
            TioShortTermDuckDBCollector
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(session.dataset_files.count(), 1)
        data_source = session.dataset_files.first()
        self.assertIn('forecast_date', data_source.metadata)
        self.assertIn('remote_url', data_source.metadata)
        self.assert_duckdb_file(data_source)

    @patch.object(CollectorSession, '_run')
    @patch('gap.ingestor.tomorrowio.threads_collector.timezone')
    @responses.activate
    def test_failed_api(self, mock_timezone, mock_run):
        """Testing collector."""
        self._mock_request(PatchRequest(
            f'https://api.tomorrow.io/v4/timelines?apikey={self.api_key}',
            response={
                'type': 'invalid_request_error',
                'message': 'Invalid request',
            },
            request_method='POST',
            status_code=400
        ))
        today = datetime(
            2024, 10, 1, 6, 0, 0
        )
        today = timezone.make_aware(
            today, timezone.get_default_timezone()
        )
        mock_timezone.now.return_value = today
        GridFactory(
            geometry=Polygon(
                (
                    (0, 0), (0, 0.01), (0.01, 0.01), (0.01, 0), (0, 0)
                )
            )
        )
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1
            }
        )
        mock_run.side_effect = partial(
            mock_collector_run,
            session,
            TioShortTermDuckDBCollector
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(session.dataset_files.count(), 1)
        self.assert_empty_duckdb_file(session.dataset_files.first())


class TioHourlyShortTermDuckDBCollectorTest(
    BaseTestWithPatchResponses, HourlyDuckDBAssert, TransactionTestCase
):
    """Tio Hourly DuckDB Collector test case."""

    fixtures = [
        '1.object_storage_manager.json',
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.HOURLY_TOMORROWIO
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
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
            store_type=DatasetStore.ZARR
        )

    @property
    def mock_requests(self):
        """Mock requests."""
        return [
            # Devices API
            PatchRequest(
                f'https://api.tomorrow.io/v4/timelines?apikey={self.api_key}',
                file_response=os.path.join(
                    self.responses_folder, 'hourly.json'
                ),
                request_method='POST'
            )
        ]

    @patch.object(CollectorSession, '_run')
    @patch('gap.ingestor.tomorrowio.threads_collector.timezone')
    @responses.activate
    def test_collector_one_grid_start_new(self, mock_timezone, mock_run):
        """Testing collector."""
        self.init_mock_requests()
        today = datetime(
            2025, 4, 24, 6, 0, 0
        )
        today = timezone.make_aware(
            today, timezone.get_default_timezone()
        )
        mock_timezone.now.return_value = today
        GridFactory(
            geometry=Polygon(
                (
                    (0, 0), (0, 0.01), (0.01, 0.01), (0.01, 0), (0, 0)
                )
            )
        )
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1,
                'grid_batch_size': 1
            }
        )
        mock_run.side_effect = partial(
            mock_collector_run,
            session,
            TioShortTermHourlyDuckDBCollector
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(session.dataset_files.count(), 1)
        data_source = session.dataset_files.first()
        self.assertIn('forecast_date', data_source.metadata)
        self.assertIn('remote_url', data_source.metadata)
        self.assert_duckdb_file(data_source)

    def test_assert_init_dates(self):
        """Test init dates."""
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1,
                'grid_batch_size': 1
            }
        )
        collector = TioShortTermHourlyCollector(session)
        today = datetime(2025, 4, 24, 0, 0, 0)
        collector._init_dates(today)
        self.assertEqual(
            collector.start_dt,
            datetime(2025, 4, 25, 0, 0, 0)
        )
        self.assertEqual(
            collector.end_dt,
            datetime(2025, 4, 29, 0, 0, 0)
        )
        self.assertEqual(collector.forecast_date, today)


class TioShortTermAsyncCollectorTest(DailyDuckDBAssert, TestCase):
    """Tio Async Collector test case."""

    fixtures = [
        '1.object_storage_manager.json',
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
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        self.response = json.loads(
            open(os.path.join(self.responses_folder, 'test.json'), "r").read()
        )

    @patch("aiohttp.ClientSession.post")
    @patch.object(CollectorSession, '_run')
    def test_collector_one_grid(self, mock_run, mock_post):
        """Testing collector."""
        today = datetime(
            2024, 10, 1, 6, 0, 0
        )
        today = timezone.make_aware(
            today, timezone.get_default_timezone()
        )
        grid = GridFactory(
            geometry=Polygon(
                (
                    (0, 0), (0, 0.01), (0.01, 0.01), (0.01, 0), (0, 0)
                )
            )
        )
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1
            }
        )
        # create DataSourceFile for the session
        data_source = DataSourceFile.objects.create(
            name=f'{str(uuid.uuid4())}.duckdb',
            dataset=self.dataset,
            start_date_time=today,
            end_date_time=today,
            format=DatasetStore.DUCKDB,
            created_on=timezone.now(),
            metadata={
                'forecast_date': (
                    today.isoformat()
                ),
                'total_grid': 1,
                'start_grid_id': grid.id,
                'end_grid_id': grid.id,
            }
        )
        session.dataset_files.set([data_source])

        mock_response = AsyncMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.status = 200
        mock_response.json.return_value = self.response

        mock_post.return_value = mock_response

        mock_run.side_effect = partial(
            mock_collector_run_with_dt,
            session,
            today,
            TioShortTermDailyCollector
        )

        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(session.dataset_files.count(), 1)
        data_source = session.dataset_files.first()
        self.assertIn('forecast_date', data_source.metadata)
        self.assertIn('remote_url', data_source.metadata)
        self.assert_duckdb_file(data_source)

    @patch("aiohttp.ClientSession.post")
    @patch.object(CollectorSession, '_run')
    def test_collector_one_grid_filtered(self, mock_run, mock_post):
        """Testing collector."""
        today = datetime(
            2024, 10, 1, 6, 0, 0
        )
        today = timezone.make_aware(
            today, timezone.get_default_timezone()
        )
        country_1 = Country.objects.create(
            name='Country 1',
            iso_a3='C1',
        )
        grid_1 = GridFactory(
            geometry=Polygon(
                (
                    (0, 0), (0, 0.01), (0.01, 0.01), (0.01, 0), (0, 0)
                )
            ),
            country=country_1
        )
        country_2 = Country.objects.create(
            name='Country 2',
            iso_a3='C2',
        )
        GridFactory(
            geometry=Polygon(
                (
                    (1, 1), (1, 1.01), (1.01, 1.01), (1.01, 1), (1, 1)
                )
            ),
            country=country_2
        )
        session = CollectorSession.objects.create(
            ingestor_type=self.ingestor_type,
            additional_config={
                'duckdb_num_threads': 1,
                'countries': [country_1.name]
            }
        )
        # create DataSourceFile for the session
        data_source = DataSourceFile.objects.create(
            name=f'{str(uuid.uuid4())}.duckdb',
            dataset=self.dataset,
            start_date_time=today,
            end_date_time=today,
            format=DatasetStore.DUCKDB,
            created_on=timezone.now(),
            metadata={
                'forecast_date': (
                    today.isoformat()
                ),
                'total_grid': 1,
                'start_grid_id': grid_1.id,
                'end_grid_id': grid_1.id,
            }
        )
        session.dataset_files.set([data_source])

        mock_response = AsyncMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.status = 200
        mock_response.json.return_value = self.response

        mock_post.return_value = mock_response

        mock_run.side_effect = partial(
            mock_collector_run_with_dt,
            session,
            today,
            TioShortTermDailyCollector
        )

        session.run()
        session.refresh_from_db()
        self.assertEqual(session.dataset_files.count(), 1)
        print(session.notes)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(session.dataset_files.count(), 1)
        data_source = session.dataset_files.first()
        self.assertIn('forecast_date', data_source.metadata)
        self.assertIn('remote_url', data_source.metadata)
        self.assert_duckdb_file(data_source)
