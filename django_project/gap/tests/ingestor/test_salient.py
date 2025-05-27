# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Ingestor.
"""

import os
import uuid
from unittest.mock import patch, MagicMock
from datetime import datetime, date
import numpy as np
import pandas as pd
from xarray.core.dataset import Dataset as xrDataset
from django.test import TestCase
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage


from gap.models import Dataset, DataSourceFile, DatasetStore
from gap.models.ingestor import (
    IngestorSession,
    IngestorType,
    CollectorSession,
    IngestorSessionStatus,
    IngestorSessionProgress
)
from gap.ingestor.salient import SalientIngestor, SalientCollector
from gap.factories import DataSourceFileFactory, DataSourceFileCacheFactory
from gap.tasks.collector import run_salient_collector_session
from tempfile import NamedTemporaryFile


LAT_METADATA = {
    'min': -27,
    'max': 16,
    'inc': 0.25,
    'original_min': -0.625
}
LON_METADATA = {
    'min': 21.8,
    'max': 52,
    'inc': 0.25,
    'original_min': 33.38
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
    forecast_day_indices = np.arange(0, 275, 1)
    empty_shape = (
        1,
        50,
        len(forecast_day_indices),
        len(new_lat),
        len(new_lon)
    )
    data_vars = {
        'temp': (
            ['forecast_date', 'ensemble', 'forecast_day_idx', 'lat', 'lon'],
            np.full(empty_shape, np.nan)
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


class SalientIngestorBaseTest(TestCase):
    """Base test for Salient ingestor/collector."""

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

    def setUp(self):
        """Set SalientIngestorBaseTest."""
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')


class TestSalientCollector(SalientIngestorBaseTest):
    """Salient collector test case."""

    @patch('gap.ingestor.salient.s3fs.S3FileSystem')
    def setUp(
        self, mock_s3fs
    ):
        """Initialize TestSalientCollector."""
        super().setUp()

        # Mock S3FileSystem object
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        # Mock session
        self.session = CollectorSession.objects.create(
            ingestor_type=IngestorType.SALIENT
        )
        self.session.additional_config = {
            'forecast_date': '2024-08-28',
            'variable_list': ['precip', 'tmin'],
            'coords': [(0, 0), (1, 1), (1, 0), (0, 1)]
        }

        self.collector = SalientCollector(
            session=self.session, working_dir='/tmp')

    def test_init(self):
        """Test initialization of SalientCollector."""
        self.assertIsNotNone(self.collector.dataset)
        self.assertEqual(self.collector.total_count, 0)
        self.assertEqual(self.collector.data_files, [])
        self.assertEqual(self.collector.metadata, {})
        self.assertIsInstance(self.collector.fs, MagicMock)

    @patch('gap.ingestor.salient.datetime.datetime')
    def test_convert_forecast_date_today(self, mock_datetime):
        """Test converting '-today' forecast date."""
        mock_datetime.now.return_value = datetime(2024, 8, 28)
        result = self.collector._convert_forecast_date('-today')
        self.assertEqual(result, date(2024, 8, 28))

    def test_convert_forecast_date_specific_date(self):
        """Test converting specific date string to date object."""
        result = self.collector._convert_forecast_date('2024-08-28')
        self.assertEqual(result, date(2024, 8, 28))

    @patch('os.stat')
    @patch('uuid.uuid4')
    def test_store_as_netcdf_file(
        self, mock_uuid4,
        mock_os_stat
    ):
        """Test storing the downscaled Salient NetCDF as a file."""
        mock_uuid4.return_value = '1234-5678'
        # mock_get_netcdf_base_url.return_value = 's3://fake-bucket/'
        mock_os_stat.return_value.st_size = 1048576

        self.collector._store_as_netcdf_file('fake_path.nc', '2024-08-28')

        self.assertEqual(self.collector.total_count, 1)
        self.assertEqual(len(self.collector.data_files), 1)
        self.assertEqual(self.collector.metadata['filesize'], 1048576)
        self.assertEqual(
            self.collector.metadata['forecast_date'], '2024-08-28')
        self.assertEqual(self.collector.metadata['end_date'], '2025-05-30')

    @patch('gap.ingestor.salient.sk')
    def test_run(self, mock_sk):
        """Test running the Salient Data Collector."""
        mock_sk.downscale.return_value = 'fake_forecast_file.nc'
        mock_sk.upload_shapefile.return_value = 'fake_shapefile'

        with (
            patch.object(self.collector, '_store_as_netcdf_file')
        ) as mock_store_as_netcdf_file:
            self.collector.run()
            mock_store_as_netcdf_file.assert_called_once()

    @patch('gap.ingestor.salient.logger')
    @patch('gap.ingestor.salient.sk')
    def test_run_with_exception(self, mock_sk, mock_logger):
        """Test running the collector with an exception."""
        mock_sk.downscale.side_effect = Exception("Test Exception")

        with self.assertRaises(Exception):
            self.collector.run()
        self.assertEqual(mock_logger.error.call_count, 2)

    @patch('gap.models.ingestor.CollectorSession.dataset_files')
    @patch('gap.models.ingestor.CollectorSession.run')
    @patch('gap.tasks.ingestor.run_ingestor_session.delay')
    def test_run_salient_collector_session(
        self, mock_ingestor, mock_collector, mock_count
    ):
        """Test run salient collector session."""
        mock_count.count.return_value = 0
        run_salient_collector_session()
        # assert
        mock_collector.assert_called_once()
        mock_ingestor.assert_not_called()

        mock_collector.reset_mock()
        mock_ingestor.reset_mock()
        # test with collector result
        mock_count.count.return_value = 1
        run_salient_collector_session()

        # assert
        session = IngestorSession.objects.filter(
            ingestor_type=IngestorType.SALIENT,
        ).order_by('id').last()
        self.assertTrue(session)
        self.assertEqual(session.collectors.count(), 1)
        mock_collector.assert_called_once()
        mock_ingestor.assert_called_once_with(session.id)

    @patch("gap.tasks.collector.notify_collector_failure.delay")
    @patch('gap.models.ingestor.CollectorSession.objects.create')
    @patch('gap.tasks.ingestor.run_ingestor_session.delay')
    def test_run_salient_collector_session_with_notify_failed(
        self, mock_ingestor, mock_create, mock_notify
    ):
        """Test run salient collector session."""
        mock_session = MagicMock()
        mock_session.id = 100
        mock_session.run.return_value = None  # No exception
        mock_session.status = IngestorSessionStatus.FAILED
        mock_session.notes = "Failure reason"
        mock_session.dataset_files.count.return_value = 0
        mock_create.return_value = mock_session

        run_salient_collector_session()
        mock_create.assert_called_once()
        mock_ingestor.assert_not_called()
        mock_notify.assert_called_once_with(100, "Failure reason")


class TestSalientIngestor(SalientIngestorBaseTest):
    """Salient ingestor test case."""

    @patch(
        'core.models.object_storage_manager.ObjectStorageManager.'
        'get_s3_env_vars'
    )
    def test_init_with_existing_source(self, mock_get_s3_env):
        """Test init method with existing DataSourceFile."""
        datasource = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='salient_test.zarr'
        )
        mock_get_s3_env.return_value = {
            'S3_ACCESS_KEY_ID': 'test_access_key',
            'S3_SECRET_ACCESS_KEY': 'test_secret_key',
            'S3_ENDPOINT_URL': 'https://test-endpoint.com',
        }
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SALIENT,
            additional_config={
                'datasourcefile_id': datasource.id,
                'datasourcefile_exists': True
            },
            trigger_task=False
        )
        ingestor = SalientIngestor(session)
        self.assertEqual(ingestor.s3['S3_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(ingestor.s3_options['key'], 'test_access_key')
        self.assertTrue(ingestor.datasource_file)
        self.assertEqual(ingestor.datasource_file.name, datasource.name)
        self.assertFalse(ingestor.created)

    def setUp(self):
        """Initialize TestSalientIngestor."""
        super().setUp()
        self.collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.SALIENT
        )
        self.datasourcefile = DataSourceFileFactory.create(
            dataset=self.dataset,
            name=f'{str(uuid.uuid4())}.nc',
            start_date_time=datetime.fromisoformat('2024-10-02'),
            end_date_time=datetime.fromisoformat('2024-10-02'),
            format=DatasetStore.NETCDF,
        )
        self.collector.dataset_files.set([self.datasourcefile])
        self.session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SALIENT,
            trigger_task=False
        )
        self.session.collectors.set([self.collector])
        self.ingestor = SalientIngestor(self.session, working_dir='/tmp')

    def tearDown(self):
        """Tear down test case."""
        s3_storage: S3Boto3Storage = storages["gap_products"]
        path = self.ingestor._get_s3_filepath(self.datasourcefile)
        if s3_storage.exists(path):
            s3_storage.delete(path)
        super().tearDown()

    @patch('xarray.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    @patch('s3fs.S3FileSystem')
    def test_open_dataset(
        self, mock_s3_filesystem, mock_get_s3_client_kwargs, mock_open_dataset
    ):
        """Test open xarray dataset."""
        # Set up mocks
        mock_open_dataset.return_value = MagicMock(spec=xrDataset)

        # Call the method
        self.ingestor._open_dataset(self.datasourcefile)

        # Assertions
        mock_open_dataset.assert_called_once()
        mock_s3_filesystem.assert_called_once()

    def test_update_zarr_source_file(self):
        """Test update zarr source file with forecast_date."""
        # Test new DataSourceFile creation
        mock_forecast_date = date(2024, 8, 28)
        self.ingestor.created = True
        self.ingestor._update_zarr_source_file(mock_forecast_date)
        self.ingestor.datasource_file.refresh_from_db()
        self.assertEqual(
            self.ingestor.datasource_file.start_date_time.date(),
            mock_forecast_date
        )
        self.assertEqual(
            self.ingestor.datasource_file.end_date_time.date(),
            mock_forecast_date
        )

    @patch('storages.backends.s3boto3.S3Boto3Storage.delete')
    def test_remove_temporary_source_file(
        self, mock_default_storage
    ):
        """Test removing temporary source file in s3."""
        # Set up mocks
        mock_source_file = MagicMock(spec=DataSourceFile)
        file_path = 'mock_path'

        # Call the method
        self.ingestor._remove_temporary_source_file(
            mock_source_file, file_path)

        # Assertions
        mock_default_storage.assert_called_once_with(file_path)
        mock_source_file.delete.assert_called_once()

    @patch('gap.utils.zarr.BaseZarrReader.get_zarr_base_url')
    @patch('xarray.open_zarr')
    @patch('fsspec.get_mapper')
    def test_verify(
        self, mock_get_mapper, mock_open_zarr, mock_get_zarr_base_url
    ):
        """Test verify salient zarr file in s3."""
        # Set up mocks
        mock_open_zarr.return_value = MagicMock(spec=xrDataset)

        # Call the method
        self.ingestor.verify()

        # Assertions
        mock_get_zarr_base_url.assert_called_once()
        mock_get_mapper.assert_called_once()
        mock_open_zarr.assert_called_once()

    @patch('gap.ingestor.salient.execute_dask_compute')
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
            # created is True
            self.ingestor.created = True
            self.assertFalse(self.ingestor._is_date_in_zarr(date(2024, 10, 2)))
            self.ingestor.created = False
            self.assertTrue(self.ingestor._is_date_in_zarr(date(2024, 10, 2)))
            mock_open.assert_called_once()
            mock_open.reset_mock()
            self.assertFalse(self.ingestor._is_date_in_zarr(date(2024, 10, 1)))
            mock_open.assert_not_called()

    def _get_file_remote_url(self, filename):
        # use gap products dir prefix
        output_url = os.environ.get(
            'GAP_S3_PRODUCTS_DIR_PREFIX', '')
        if not output_url.endswith('/'):
            output_url += '/'
        output_url += os.path.join(
            'salient_collector',
            filename
        )

        return output_url

    @patch('xarray.Dataset.to_zarr')
    def test_run(self, mock_dask_compute):
        """Test Run Salient Ingestor."""
        self.ingestor.created = False
        self.ingestor.variables = ['temp']
        # Mock the open_dataset return value
        mock_dataset = xrDataset(
            {
                'temp': (
                    ('ensemble', 'forecast_day', 'lat', 'lon'),
                    np.random.rand(50, 275, 2, 2)
                ),
            },
            coords={
                'forecast_date': pd.date_range('2024-10-02', periods=1),
                'forecast_day': pd.date_range('2024-10-02', periods=275),
                'lat': [-27, -26.75],
                'lon': [21.8, 22.05],
            }
        )
        path = self._get_file_remote_url(
            self.datasourcefile.name
        )
        s3_storage: S3Boto3Storage = storages["gap_products"]
        with NamedTemporaryFile() as tmp_file:
            # Create a temporary NetCDF file
            mock_dataset.to_netcdf(tmp_file.name)
            # store to s3 using default storage
            s3_storage.save(path, tmp_file)

        self.assertTrue(self.ingestor._check_netcdf_file_exists(
            self.ingestor._get_s3_filepath(self.datasourcefile)
        ))

        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            # Call the method
            self.ingestor._run()

        # Assertions
        mock_dask_compute.assert_called_once()
        self.assertEqual(self.ingestor.metadata['total_files'], 1)
        self.assertEqual(
            IngestorSessionProgress.objects.filter(
                session=self.session
            ).count(),
            1
        )
        self.assertFalse(self.ingestor._check_netcdf_file_exists(
            self.ingestor._get_s3_filepath(self.datasourcefile)
        ))

    def test_invalidate_cache(self):
        """Test invalidate cache function."""
        cache_file = DataSourceFileCacheFactory.create(
            source_file=self.ingestor.datasource_file
        )
        self.ingestor._invalidate_zarr_cache()
        cache_file.refresh_from_db()
        self.assertIsNotNone(cache_file.expired_on)
