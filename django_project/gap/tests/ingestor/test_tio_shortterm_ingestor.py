# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tio Shortterm Ingestor.
"""

import os
import json
import uuid
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timedelta
import zipfile
import duckdb
import numpy as np
import pandas as pd
import dask.array as da
from xarray.core.dataset import Dataset as xrDataset
from django.test import TestCase
from django.contrib.gis.geos import Polygon

from gap.models import Dataset, DatasetStore, DataSourceFile
from gap.models.ingestor import (
    IngestorSession,
    IngestorType,
    CollectorSession
)
from gap.ingestor.base import CoordMapping
from gap.ingestor.tomorrowio import (
    TioShortTermIngestor,
    TioShortTermDuckDBIngestor,
    TioShortTermDuckDBCollector,
    TioHourlyShortTermIngestor,
    TioShortTermHourlyDuckDBCollector
)
from gap.ingestor.exceptions import (
    MissingCollectorSessionException, FileNotFoundException,
    AdditionalConfigNotFoundException
)
from gap.factories import DataSourceFileFactory, GridFactory
from gap.tasks.collector import (
    run_tio_collector_session,
    run_tio_hourly_collector_session
)


LAT_METADATA = {
    'min': -4.65013565,
    'max': 5.46326983,
    'inc': 0.03586314,
    'original_min': -4.65013565
}
LON_METADATA = {
    'min': 33.91823667,
    'max': 41.84325607,
    'inc': 0.036353,
    'original_min': 33.91823667
}


def mock_open_zarr_dataset():
    """Mock open zarr dataset."""
    new_lat = np.arange(
        LAT_METADATA['min'], LAT_METADATA['max'] + LAT_METADATA['inc'],
        LAT_METADATA['inc']
    )
    new_lon = np.arange(
        LON_METADATA['min'], LON_METADATA['max'] + LON_METADATA['inc'],
        LON_METADATA['inc']
    )

    # Create the Dataset
    forecast_date_array = pd.date_range(
        '2024-10-02', periods=1)
    forecast_day_indices = np.arange(-6, 15, 1)
    empty_shape = (1, 21, len(new_lat), len(new_lon))
    chunks = (1, 21, 150, 110)
    data_vars = {
        'max_temperature': (
            ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
            da.empty(empty_shape, chunks=chunks)
        )
    }
    return xrDataset(
        data_vars=data_vars,
        coords={
            'forecast_date': ('forecast_date', forecast_date_array),
            'forecast_day_idx': (
                'forecast_day_idx', forecast_day_indices),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }
    )


def mock_hourly_open_zarr_dataset():
    """Mock open zarr dataset."""
    new_lat = np.arange(
        LAT_METADATA['min'], LAT_METADATA['max'] + LAT_METADATA['inc'],
        LAT_METADATA['inc']
    )
    new_lon = np.arange(
        LON_METADATA['min'], LON_METADATA['max'] + LON_METADATA['inc'],
        LON_METADATA['inc']
    )

    # Create the Dataset
    forecast_date_array = pd.date_range(
        '2025-04-24', periods=1)
    forecast_day_indices = np.arange(1, 5, 1)
    times = np.array([np.timedelta64(h, 'h') for h in range(24)])
    empty_shape = (
        1, len(forecast_day_indices), 24,
        len(new_lat), len(new_lon)
    )
    chunks = (1, len(forecast_day_indices), 24, 20, 20)
    data_vars = {
        'temperature': (
            ['forecast_date', 'forecast_day_idx', 'time', 'lat', 'lon'],
            da.empty(empty_shape, chunks=chunks)
        )
    }
    return xrDataset(
        data_vars=data_vars,
        coords={
            'forecast_date': ('forecast_date', forecast_date_array),
            'forecast_day_idx': (
                'forecast_day_idx', forecast_day_indices),
            'time': ('time', times),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }
    )


def create_polygon():
    """Create mock polygon for Grid."""
    return Polygon.from_bbox([
        LON_METADATA['min'],
        LAT_METADATA['min'],
        LON_METADATA['min'] + 2 * LON_METADATA['inc'],
        LAT_METADATA['min'] + 2 * LON_METADATA['inc'],
    ])


class TestTioIngestor(TestCase):
    """Tomorrow.io ingestor test case."""

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

    @patch('gap.utils.zarr.BaseZarrReader')
    def setUp(self, mock_zarr_reader):
        """Initialize TestTioIngestor."""
        super().setUp()
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        self.collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.TIO_FORECAST_COLLECTOR
        )
        self.datasourcefile = DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2024-10-02.zip',
            format=DatasetStore.ZIP_FILE,
            metadata={
                'forecast_date': '2024-10-02'
            }
        )
        self.collector.dataset_files.set([self.datasourcefile])
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name=f'tio_{str(uuid.uuid4())}.zarr'
        )
        self.session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            trigger_task=False,
            additional_config={
                'datasourcefile_id': self.zarr_source.id,
                'datasourcefile_exists': True
            }
        )
        self.session.collectors.set([self.collector])

        self.mock_zarr_reader = mock_zarr_reader
        # self.mock_netcdf_media_s3 = mock_netcdf_media_s3
        # self.mock_dask_compute = mock_dask_compute

        self.ingestor = TioShortTermIngestor(self.session)
        self.ingestor.lat_metadata = LAT_METADATA
        self.ingestor.lon_metadata = LON_METADATA

    @patch(
        'core.models.object_storage_manager.ObjectStorageManager.'
        'get_s3_env_vars'
    )
    def test_init_with_existing_source(self, mock_get_s3_env):
        """Test init method with existing DataSourceFile."""
        datasource = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='tio_test.zarr'
        )
        mock_get_s3_env.return_value = {
            'S3_ACCESS_KEY_ID': 'test_access_key',
            'S3_SECRET_ACCESS_KEY': 'test_secret_key',
            'S3_ENDPOINT_URL': 'https://test-endpoint.com',
        }
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            additional_config={
                'datasourcefile_id': datasource.id,
                'datasourcefile_exists': True,
                'remove_temp_file': False
            },
            trigger_task=False
        )
        ingestor = TioShortTermIngestor(session)
        self.assertEqual(ingestor.s3['S3_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(ingestor.s3_options['key'], 'test_access_key')
        self.assertTrue(ingestor.datasource_file)
        self.assertEqual(ingestor.datasource_file.name, datasource.name)
        self.assertFalse(ingestor.created)

    def test_run_with_exception(self):
        """Test exception during run."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            trigger_task=False
        )
        ingestor = TioShortTermIngestor(session)
        with self.assertRaises(MissingCollectorSessionException) as context:
            ingestor._run()
        self.assertTrue(
            'Missing collector session' in context.exception.message)

        collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.TIO_FORECAST_COLLECTOR
        )
        session.collectors.set([collector])
        with self.assertRaises(FileNotFoundException) as context:
            ingestor._run()
        self.assertTrue('File not found.' in context.exception.message)

        datasource = DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2024-10-02.zip',
            metadata={}
        )
        collector.dataset_files.set([datasource])
        with self.assertRaises(AdditionalConfigNotFoundException) as context:
            ingestor.run()
        self.assertTrue('metadata.forecast_date' in context.exception.message)

    @patch('gap.ingestor.tomorrowio.json_ingestor.execute_dask_compute')
    def test_append_new_forecast_date(self, mock_dask_compute):
        """Test append new forecast date method."""
        forecast_date = date(2024, 10, 1)
        self.ingestor._append_new_forecast_date(forecast_date, True)
        mock_dask_compute.assert_called_once()

        mock_dask_compute.reset_mock()
        forecast_date = date(2024, 10, 2)
        self.ingestor._append_new_forecast_date(forecast_date, False)
        mock_dask_compute.assert_called_once()

    def test_is_date_in_zarr(self):
        """Test check date in zarr function."""
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.assertTrue(self.ingestor._is_date_in_zarr(date(2024, 10, 2)))
            mock_open.assert_called_once()
            mock_open.reset_mock()
            self.assertFalse(self.ingestor._is_date_in_zarr(date(2024, 10, 1)))
            mock_open.assert_not_called()
            # created is True
            self.ingestor.created = True
            self.assertFalse(self.ingestor._is_date_in_zarr(date(2024, 10, 2)))
            self.ingestor.created = False

    def test_is_sorted_and_incremented(self):
        """Test is_sorted_and_incremented function."""
        arr = None
        self.assertFalse(self.ingestor._is_sorted_and_incremented(arr))
        arr = [1]
        self.assertTrue(self.ingestor._is_sorted_and_incremented(arr))
        arr = [1, 2, 5, 7]
        self.assertFalse(self.ingestor._is_sorted_and_incremented(arr))
        arr = [1, 2, 3, 4, 5, 6]
        self.assertTrue(self.ingestor._is_sorted_and_incremented(arr))

    def test_transform_coordinates_array(self):
        """Test transform_coordinates_array function."""
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            lat_arr = []
            for i in range(10):
                lat_arr.append(LAT_METADATA['min'] + i * LAT_METADATA['inc'])
            coords = self.ingestor._transform_coordinates_array(lat_arr, 'lat')
            mock_open.assert_called_once()
            self.assertEqual(len(coords), 10)
            self.assertTrue(
                self.ingestor._is_sorted_and_incremented(
                    [c.nearest_idx for c in coords]
                )
            )

    def test_transform_coordinates_array_unique(self):
        """Test transform_coordinates_array function."""
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            lat_arr = [
                LAT_METADATA['min']
            ]
            for i in range(10):
                lat_arr.append(LAT_METADATA['min'] + i * LAT_METADATA['inc'])
            coords = self.ingestor._transform_coordinates_array(lat_arr, 'lat')
            mock_open.assert_called_once()
            self.assertEqual(len(coords), 10)
            self.assertTrue(
                self.ingestor._is_sorted_and_incremented(
                    [c.nearest_idx for c in coords]
                )
            )

    def test_transform_coordinates_array_missing(self):
        """Test transform_coordinates_array function."""
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            lat_arr = []
            for i in range(10):
                if i not in [2, 5, 7]:
                    lat_arr.append(
                        LAT_METADATA['min'] + i * LAT_METADATA['inc']
                    )
            coords = self.ingestor._transform_coordinates_array(
                lat_arr, 'lat', fix_incremented=True
            )
            mock_open.assert_called_once()
            self.assertEqual(len(coords), 10)
            self.assertTrue(
                self.ingestor._is_sorted_and_incremented(
                    [c.nearest_idx for c in coords]
                )
            )

    def test_find_chunk_slices(self):
        """Test find_chunk_slices function."""
        coords = self.ingestor._find_chunk_slices(1, 1)
        self.assertEqual(len(coords), 1)
        coords = self.ingestor._find_chunk_slices(150, 60)
        self.assertEqual(len(coords), 3)
        self.assertEqual(
            coords,
            [slice(0, 60), slice(60, 120), slice(120, 150)]
        )

    @patch('gap.ingestor.tomorrowio.json_ingestor.execute_dask_compute')
    def test_update_by_region(self, mock_dask_compute):
        """Test update_by_region function."""
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            forecast_date = date(2024, 10, 2)
            new_data = {
                'max_temperature': np.random.rand(1, 21, 1, 1)
            }
            lat_arr = [CoordMapping(
                LAT_METADATA['min'], 0, LAT_METADATA['min']
            )]
            lon_arr = [CoordMapping(
                LON_METADATA['min'], 0, LON_METADATA['min']
            )]
            self.ingestor._update_by_region(
                forecast_date, lat_arr, lon_arr, new_data)
            mock_dask_compute.assert_called_once()

    @patch('django.core.files.storage.default_storage.open')
    @patch('zipfile.ZipFile.namelist')
    @patch('xarray.Dataset.to_zarr')
    def test_run_success(
            self, mock_dask_compute, mock_namelist, mock_default_storage):
        """Test run ingestor succesfully."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'tio_shorterm_collector',
            'grid_data.zip'
        )
        # mock default_storage.open
        f = open(filepath, "rb")
        mock_default_storage.return_value = f
        json_f = zipfile.ZipFile(f, 'r').open('grid-1.json')

        # create a grid
        grid = GridFactory(geometry=create_polygon())
        # mock zip_file.namelist
        mock_namelist.return_value = [f'grid-{grid.id}.json']
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            with patch('zipfile.ZipFile.open') as mock_zip_open:
                mock_open.return_value = mock_open_zarr_dataset()
                mock_zip_open.return_value = json_f
                self.ingestor._run()
                mock_zip_open.assert_called_once()

        mock_default_storage.assert_called_once()
        mock_dask_compute.assert_called_once()
        self.assertEqual(self.ingestor.metadata['total_json_processed'], 1)
        self.assertEqual(len(self.ingestor.metadata['chunks']), 1)
        f.close()

    @patch('gap.utils.zarr.BaseZarrReader.get_zarr_base_url')
    @patch('xarray.open_zarr')
    @patch('fsspec.get_mapper')
    def test_verify(
        self, mock_get_mapper, mock_open_zarr, mock_get_zarr_base_url
    ):
        """Test verify Tio zarr file in s3."""
        # Set up mocks
        mock_open_zarr.return_value = MagicMock(spec=xrDataset)

        # Call the method
        self.ingestor.verify()

        # Assertions
        mock_get_zarr_base_url.assert_called_once()
        mock_get_mapper.assert_called_once()
        mock_open_zarr.assert_called_once()

    @patch('gap.models.ingestor.CollectorSession.dataset_files')
    @patch('gap.models.ingestor.CollectorSession.run')
    @patch('gap.tasks.ingestor.run_ingestor_session.delay')
    def test_run_tio_collector_session(
        self, mock_ingestor, mock_collector, mock_count
    ):
        """Test run tio collector session."""
        mock_count.count.return_value = 0
        run_tio_collector_session()
        # assert
        mock_collector.assert_called_once()
        mock_ingestor.assert_not_called()

        mock_collector.reset_mock()
        mock_ingestor.reset_mock()
        # test with collector result
        mock_count.count.return_value = 1
        run_tio_collector_session()

        # assert
        session = IngestorSession.objects.filter(
            ingestor_type=IngestorType.TOMORROWIO,
        ).order_by('id').last()
        self.assertTrue(session)
        self.assertEqual(session.collectors.count(), 1)
        mock_collector.assert_called_once()
        mock_ingestor.assert_called_once_with(session.id)

    @patch('gap.models.ingestor.CollectorSession.dataset_files')
    @patch('gap.models.ingestor.CollectorSession.run')
    @patch('gap.tasks.ingestor.run_ingestor_session.delay')
    def test_run_tio_hourly_collector_session(
        self, mock_ingestor, mock_collector, mock_count
    ):
        """Test run tio hourly collector session."""
        mock_count.count.return_value = 0
        run_tio_hourly_collector_session()
        # assert
        mock_collector.assert_called_once()
        mock_ingestor.assert_not_called()

        mock_collector.reset_mock()
        mock_ingestor.reset_mock()
        # test with collector result
        mock_count.count.return_value = 1
        run_tio_hourly_collector_session()

        # assert
        session = IngestorSession.objects.filter(
            ingestor_type=IngestorType.HOURLY_TOMORROWIO,
        ).order_by('id').last()
        self.assertTrue(session)
        self.assertEqual(session.collectors.count(), 1)
        mock_collector.assert_called_once()
        mock_ingestor.assert_called_once_with(session.id)
        config = session.additional_config
        self.assertFalse(config.get('use_latest_datasource'))
        self.assertNotIn(
            'datasourcefile_id', config
        )
        self.assertNotIn(
            'datasourcefile_exists', config
        )


class TestDuckDBTioIngestor(TestCase):
    """Tomorrow.io ingestor test case."""

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

    @patch('gap.utils.zarr.BaseZarrReader')
    def setUp(self, mock_zarr_reader):
        """Initialize TestDuckDBTioIngestor."""
        super().setUp()
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        self.collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.TIO_FORECAST_COLLECTOR
        )
        self.datasourcefile = DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2024-10-02.duckdb',
            format=DatasetStore.DUCKDB,
            metadata={
                'forecast_date': '2024-10-02'
            }
        )
        self.collector.dataset_files.set([self.datasourcefile])
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name=f'tio_{uuid.uuid4()}.zarr'
        )
        self.session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            trigger_task=False,
            additional_config={
                'datasourcefile_id': self.zarr_source.id,
                'datasourcefile_exists': True
            }
        )
        self.session.collectors.set([self.collector])

        self.mock_zarr_reader = mock_zarr_reader
        # self.mock_netcdf_media_s3 = mock_netcdf_media_s3
        # self.mock_dask_compute = mock_dask_compute

        self.ingestor = TioShortTermDuckDBIngestor(self.session)
        self.ingestor.lat_metadata = LAT_METADATA
        self.ingestor.lon_metadata = LON_METADATA

    def _create_duckdb_file(self):
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'tio_shorterm_collector',
            'grid_data.zip'
        )
        # mock default_storage.open
        f = open(filepath, "rb")

        json_f = zipfile.ZipFile(f, 'r').open('grid-1.json')

        # create a grid
        grid = GridFactory(geometry=create_polygon())

        # read the json file and create a duckdb file
        os.makedirs('/tmp/tio_collector', exist_ok=True)
        tmp_filepath = os.path.join(
            '/tmp', 'tio_collector', f'{str(uuid.uuid4())}.duckdb'
        )
        duckdb_conn = duckdb.connect(tmp_filepath)
        collector_runner = TioShortTermDuckDBCollector(self.collector)
        collector_runner._init_table(duckdb_conn)
        json_data = json.load(json_f)
        for item in json_data['data']:
            date = datetime.fromisoformat(item['datetime'])
            values = item['values']

            param = [
                grid.id,
                grid.geometry.centroid.y,
                grid.geometry.centroid.x,
                date.date()
            ]

            param_names = []
            param_placeholders = []
            for attr in collector_runner.attribute_names:
                param_names.append(attr)
                param_placeholders.append('?')
                if attr in values:
                    param.append(values[attr])
                else:
                    param.append(None)

            duckdb_conn.execute(f"""
                INSERT INTO weather (grid_id, lat, lon, date,
                    {', '.join(param_names)}
                ) VALUES (?, ?, ?, ?, {', '.join(param_placeholders)})
                """, param
            )

        duckdb_conn.close()
        self.datasourcefile.name = os.path.basename(tmp_filepath)
        collector_runner._upload_duckdb_file(self.datasourcefile)

        json_f.close()
        f.close()
        return grid

    @patch('xarray.Dataset.to_zarr')
    def test_success_ingestor(self, mock_dask_compute):
        """Test ingestor success run."""
        self._create_duckdb_file()

        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.ingestor._run()

        mock_dask_compute.assert_called_once()
        self.assertEqual(self.ingestor.metadata['total_json_processed'], 1)
        self.assertEqual(len(self.ingestor.metadata['chunks']), 1)
        self.assertEqual(self.collector.dataset_files.count(), 0)


class TestDuckDBTioHourlyIngestor(TestCase):
    """Tomorrow.io hourly ingestor test case."""

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

    @patch('gap.utils.zarr.BaseZarrReader')
    def setUp(self, mock_zarr_reader):
        """Initialize TestDuckDBTioHourlyIngestor."""
        super().setUp()
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Hourly Forecast',
            store_type=DatasetStore.ZARR
        )
        self.collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.HOURLY_TOMORROWIO
        )
        self.datasourcefile = DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2025-04-24.duckdb',
            format=DatasetStore.DUCKDB,
            metadata={
                'forecast_date': '2025-04-24'
            }
        )
        self.collector.dataset_files.set([self.datasourcefile])
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name=f'tio_{uuid.uuid4()}.zarr',
            is_latest=True
        )
        self.session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            trigger_task=False,
            additional_config={
                'datasourcefile_id': self.zarr_source.id,
                'datasourcefile_exists': True
            }
        )
        self.session.collectors.set([self.collector])

        self.mock_zarr_reader = mock_zarr_reader
        # self.mock_netcdf_media_s3 = mock_netcdf_media_s3
        # self.mock_dask_compute = mock_dask_compute

        self.ingestor = TioHourlyShortTermIngestor(self.session)
        self.ingestor.lat_metadata = LAT_METADATA
        self.ingestor.lon_metadata = LON_METADATA

    def _create_duckdb_file(self, collector, datasourcefile):
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'tio_shorterm_collector',
            'hourly.json'
        )
        # mock default_storage.open
        json_f = open(filepath, "r")

        # create a grid
        grid = GridFactory(geometry=create_polygon())

        # read the json file and create a duckdb file
        os.makedirs('/tmp/tio_collector', exist_ok=True)
        tmp_filepath = os.path.join(
            '/tmp', 'tio_collector', f'{str(uuid.uuid4())}.duckdb'
        )
        duckdb_conn = duckdb.connect(tmp_filepath)
        collector_runner = TioShortTermHourlyDuckDBCollector(collector)
        collector_runner._init_table(duckdb_conn)
        json_data = json.load(json_f)
        forecast_day_indices = range(1, 5, 1)
        for i in forecast_day_indices:
            date = datetime.fromisoformat(
                json_data['data']['timelines'][0]['intervals'][0]['startTime']
            )
            date = date + timedelta(days=i)
            for item in json_data['data']['timelines'][0]['intervals']:
                values = item['values']

                param = [
                    grid.id,
                    grid.geometry.centroid.y,
                    grid.geometry.centroid.x,
                    date.date(),
                    date.time()
                ]

                param_names = []
                param_placeholders = []
                for attr in collector_runner.attribute_names:
                    param_names.append(attr)
                    param_placeholders.append('?')
                    if attr in values:
                        param.append(values[attr])
                    else:
                        param.append(None)

                duckdb_conn.execute(f"""
                    INSERT INTO weather (grid_id, lat, lon, date, time,
                        {', '.join(param_names)}
                    ) VALUES (?, ?, ?, ?, ?, {', '.join(param_placeholders)})
                    """, param
                )

        duckdb_conn.close()
        datasourcefile.name = os.path.basename(tmp_filepath)
        collector_runner._upload_duckdb_file(datasourcefile)

        json_f.close()
        return grid

    @patch('xarray.Dataset.to_zarr')
    def test_success_ingestor(self, mock_dask_compute):
        """Test ingestor success run."""
        self._create_duckdb_file(self.collector, self.datasourcefile)

        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_hourly_open_zarr_dataset()
            self.ingestor._run()

        mock_dask_compute.assert_called_once()
        self.assertEqual(self.ingestor.metadata['total_json_processed'], 1)
        self.assertEqual(len(self.ingestor.metadata['chunks']), 1)
        self.assertEqual(self.collector.dataset_files.count(), 0)

    @patch('xarray.Dataset.to_zarr')
    def test_success_ingestor_retention(self, mock_dask_compute):
        """Test ingestor success run with retention policy."""
        collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.HOURLY_TOMORROWIO
        )
        datasourcefile = DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2025-04-24.duckdb',
            format=DatasetStore.DUCKDB,
            metadata={
                'forecast_date': '2025-04-24'
            }
        )
        collector.dataset_files.set([datasourcefile])
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            trigger_task=False,
            additional_config={
                'use_latest_datasource': False
            }
        )
        session.collectors.set([collector])

        ingestor = TioHourlyShortTermIngestor(session)
        ingestor.lat_metadata = LAT_METADATA
        ingestor.lon_metadata = LON_METADATA
        self._create_duckdb_file(collector, datasourcefile)

        with patch.object(ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_hourly_open_zarr_dataset()
            ingestor._run()

        # create new date and write region
        self.assertEqual(mock_dask_compute.call_count, 2)
        self.assertEqual(ingestor.metadata['total_json_processed'], 1)
        self.assertEqual(len(ingestor.metadata['chunks']), 1)
        self.assertEqual(collector.dataset_files.count(), 0)
        # ensure self.zarr_source has deleted_at
        self.zarr_source.refresh_from_db()
        self.assertIsNotNone(self.zarr_source.deleted_at)
        self.assertFalse(self.zarr_source.is_latest)
        # find the latest datasourcefile
        latest_datasourcefile = DataSourceFile.objects.filter(
            dataset=self.dataset,
            is_latest=True,
            format=DatasetStore.ZARR
        ).first()
        self.assertIsNotNone(latest_datasourcefile)
        self.assertNotEqual(
            latest_datasourcefile.id, self.zarr_source.id
        )
