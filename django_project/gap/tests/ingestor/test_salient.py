# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Ingestor.
"""

from unittest.mock import patch, MagicMock
from datetime import datetime, date
import numpy as np
import pandas as pd
from xarray.core.dataset import Dataset as xrDataset
from django.test import TestCase

from gap.models import Dataset, DataSourceFile, DatasetStore
from gap.models.ingestor import (
    IngestorSession,
    IngestorType,
    CollectorSession,
    IngestorSessionStatus
)
from gap.ingestor.salient import SalientIngestor, SalientCollector
from gap.factories import DataSourceFileFactory, DataSourceFileCacheFactory
from gap.tasks.collector import run_salient_collector_session


class SalientIngestorBaseTest(TestCase):
    """Base test for Salient ingestor/collector."""

    fixtures = [
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

    @patch('gap.ingestor.salient.NetCDFMediaS3.get_s3_variables')
    @patch('gap.ingestor.salient.NetCDFMediaS3.get_s3_client_kwargs')
    @patch('gap.ingestor.salient.s3fs.S3FileSystem')
    def setUp(
        self, mock_s3fs, mock_get_s3_client_kwargs, mock_get_s3_variables
    ):
        """Initialize TestSalientCollector."""
        super().setUp()
        # Mock S3 variables
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'fake-access-key',
            'AWS_SECRET_ACCESS_KEY': 'fake-secret-key'
        }

        # Mock S3 client kwargs
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://fake-s3-endpoint'
        }

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

    @patch('xarray.open_dataset')
    @patch('os.stat')
    @patch('uuid.uuid4')
    @patch('gap.utils.netcdf.NetCDFMediaS3.get_netcdf_base_url')
    def test_store_as_netcdf_file(
        self, mock_get_netcdf_base_url, mock_uuid4,
        mock_os_stat, mock_open_dataset
    ):
        """Test storing the downscaled Salient NetCDF as a file."""
        mock_dataset = MagicMock()
        mock_open_dataset.return_value = mock_dataset
        mock_uuid4.return_value = '1234-5678'
        mock_get_netcdf_base_url.return_value = 's3://fake-bucket/'
        mock_os_stat.return_value.st_size = 1048576

        self.collector._store_as_netcdf_file('fake_path.nc', '2024-08-28')

        self.assertEqual(self.collector.total_count, 1)
        self.assertEqual(len(self.collector.data_files), 1)
        self.assertEqual(self.collector.metadata['filesize'], 1048576)
        self.assertEqual(
            self.collector.metadata['forecast_date'], '2024-08-28')
        self.assertEqual(self.collector.metadata['end_date'], '2024-11-26')

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

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_init_with_existing_source(
        self, mock_get_s3_client_kwargs, mock_get_s3_variables
    ):
        """Test init method with existing DataSourceFile."""
        datasource = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='salient_test.zarr'
        )
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
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
        self.assertEqual(ingestor.s3['AWS_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(ingestor.s3_options['key'], 'test_access_key')
        self.assertTrue(ingestor.datasource_file)
        self.assertEqual(ingestor.datasource_file.name, datasource.name)
        self.assertFalse(ingestor.created)

    @patch('gap.utils.netcdf.NetCDFMediaS3')
    @patch('gap.utils.zarr.BaseZarrReader')
    def setUp(self, mock_zarr_reader, mock_netcdf_media_s3):
        """Initialize TestSalientIngestor."""
        super().setUp()
        self.collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.SALIENT
        )
        self.datasourcefile = DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2023-01-02.nc'
        )
        self.collector.dataset_files.set([self.datasourcefile])
        self.session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SALIENT,
            trigger_task=False
        )
        self.session.collectors.set([self.collector])


        self.mock_zarr_reader = mock_zarr_reader
        self.mock_netcdf_media_s3 = mock_netcdf_media_s3

        self.ingestor = SalientIngestor(self.session, working_dir='/tmp')

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

    @patch('gap.utils.netcdf.NetCDFMediaS3')
    @patch('s3fs.S3FileSystem')
    @patch('django.core.files.storage.default_storage.delete')
    def test_remove_temporary_source_file(
        self, mock_default_storage, mock_s3_filesystem, mock_netcdf_media_s3
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
    @patch('xarray.Dataset.to_zarr')
    @patch('gap.utils.zarr.BaseZarrReader.get_zarr_base_url')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    def test_store_as_zarr(
        self, mock_get_s3_variables, mock_get_zarr_base_url,
        mock_to_zarr, mock_compute
    ):
        """Test store the dataset to zarr."""
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'mock_key',
            'AWS_SECRET_ACCESS_KEY': 'mock_secret'
        }
        mock_get_zarr_base_url.return_value = 'mock_zarr_url/'

        # Create a mock dataset
        mock_dataset = xrDataset(
            {
                'temp': (
                    ('forecast_day', 'ensemble', 'lat', 'lon'),
                    np.random.rand(1, 50, 2, 2)
                ),
            },
            coords={
                'forecast_day': pd.date_range('2024-08-28', periods=1),
                'lat': [0, 1],
                'lon': [0, 1],
            }
        )
        forecast_date = date(2024, 8, 28)

        # Call the method
        self.ingestor.created = True
        self.ingestor.store_as_zarr(mock_dataset, forecast_date)

        # Assertions
        mock_to_zarr.assert_called_once()
        self.assertEqual(mock_to_zarr.call_args[1]['mode'], 'w')
        self.assertTrue(mock_to_zarr.call_args[1]['consolidated'])
        self.assertEqual(
            mock_to_zarr.call_args[1]['storage_options'],
            self.ingestor.s3_options
        )
        mock_compute.assert_called_once()

    @patch('gap.ingestor.salient.execute_dask_compute')
    @patch('xarray.Dataset.to_zarr')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_zarr_base_url')
    @patch('xarray.open_dataset')
    @patch('s3fs.S3FileSystem')
    @patch('django.core.files.storage.default_storage.exists')
    @patch('django.core.files.storage.default_storage.delete')
    def test_run(
        self, mock_storage_delete, mock_storage_exists, mock_s3_filesystem,
        mock_open_dataset, mock_get_zarr_base_url, mock_get_s3_variables,
        mock_to_zarr, mock_compute
    ):
        """Test Run Salient Ingestor."""
        # Set up mocks
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'mock_key',
            'AWS_SECRET_ACCESS_KEY': 'mock_secret'
        }
        mock_get_zarr_base_url.return_value = 'mock_zarr_url/'

        # Mock the default storage to simulate file existence
        mock_storage_exists.return_value = True

        # Mock the open_dataset return value
        mock_dataset = xrDataset(
            {
                'temp': (
                    ('forecast_day', 'ensemble', 'lat', 'lon'),
                    np.random.rand(1, 50, 2, 2)
                ),
            },
            coords={
                'forecast_day': pd.date_range('2024-08-28', periods=1),
                'lat': [0, 1],
                'lon': [0, 1],
            }
        )
        mock_open_dataset.return_value = mock_dataset

        # Call the method
        self.ingestor._run()

        # Assertions
        self.assertEqual(self.ingestor.metadata['total_files'], 1)
        self.assertEqual(len(self.ingestor.metadata['forecast_dates']), 1)
        mock_storage_delete.assert_called_once_with(
            self.ingestor._get_s3_filepath(self.datasourcefile))
        mock_to_zarr.assert_called_once()
        mock_compute.assert_called_once()

    def test_invalidate_cache(self):
        """Test invalidate cache function."""
        cache_file = DataSourceFileCacheFactory.create(
            source_file=self.ingestor.datasource_file
        )
        self.ingestor._invalidate_zarr_cache()
        cache_file.refresh_from_db()
        self.assertIsNotNone(cache_file.expired_on)
