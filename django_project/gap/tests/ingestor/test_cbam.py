# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for CBAM Ingestor.
"""

from unittest.mock import patch, MagicMock
from datetime import datetime, date, time
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
from gap.ingestor.cbam import CBAMIngestor
from gap.factories import DataSourceFileFactory
from gap.utils.netcdf import find_start_latlng


class CBAMIngestorBaseTest(TestCase):
    """Base test for CBAM ingestor/collector."""

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
        """Set CBAMIngestorBaseTest."""
        self.dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')


class CBAMCollectorTest(CBAMIngestorBaseTest):
    """CBAM collector test case."""

    @patch('gap.ingestor.cbam.s3fs.S3FileSystem')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_variables')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_client_kwargs')
    def test_cbam_collector(
        self, mock_get_s3_kwargs, mock_get_s3_env, mock_s3fs
    ):
        """Test run cbam collector."""
        mock_get_s3_env.return_value = {
            'AWS_DIR_PREFIX': 'cbam',
            'AWS_ENDPOINT_URL': 'test_endpoint',
            'AWS_BUCKET_NAME': 'test_bucket'
        }
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.walk.return_value = [
            ('test_bucket/cbam', [], ['2023-01-01.nc']),
            ('test_bucket/cbam', [], ['2023-01-02.nc']),
            ('test_bucket/cbam/dmrpp', [], ['2023-01-01.nc.dmrpp']),
            ('test_bucket/cbam/2023', [], ['2023-02-01.nc']),
        ]
        # add existing NetCDF File
        DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2023-01-02.nc'
        )
        collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.CBAM
        )
        collector.run()
        collector.refresh_from_db()
        self.assertEqual(collector.status, IngestorSessionStatus.SUCCESS)
        mock_fs.walk.assert_called_with('s3://test_bucket/cbam')
        self.assertEqual(
            DataSourceFile.objects.filter(
                dataset=self.dataset, name='2023-01-02.nc'
            ).count(),
            1
        )
        self.assertFalse(
            DataSourceFile.objects.filter(
                dataset=self.dataset, name='dmrpp/2023-01-01.nc.dmrpp'
            ).exists()
        )
        self.assertTrue(
            DataSourceFile.objects.filter(
                dataset=self.dataset, name='2023-01-01.nc'
            ).exists()
        )
        self.assertTrue(
            DataSourceFile.objects.filter(
                dataset=self.dataset, name='2023/2023-02-01.nc'
            ).exists()
        )

    @patch('gap.ingestor.cbam.s3fs.S3FileSystem')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_variables')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_client_kwargs')
    def test_cbam_collector_cancel(
        self, mock_get_s3_kwargs, mock_get_s3_env, mock_s3fs
    ):
        """Test run cbam collector."""
        mock_get_s3_env.return_value = {
            'AWS_DIR_PREFIX': 'cbam',
            'AWS_ENDPOINT_URL': 'test_endpoint',
            'AWS_BUCKET_NAME': 'test_bucket'
        }
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.walk.return_value = [
            ('test_bucket/cbam', [], ['2023-01-01.nc']),
            ('test_bucket/cbam', [], ['2023-01-02.nc']),
            ('test_bucket/cbam/dmrpp', [], ['2023-01-01.nc.dmrpp']),
            ('test_bucket/cbam/2023', [], ['2023-02-01.nc']),
        ]
        # add existing NetCDF File
        DataSourceFileFactory.create(
            dataset=self.dataset,
            name='2023-01-02.nc'
        )
        collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.CBAM
        )
        collector.is_cancelled = True
        collector.save()
        collector.run()
        collector.refresh_from_db()
        self.assertEqual(collector.status, IngestorSessionStatus.CANCELLED)
        mock_fs.walk.assert_called_with('s3://test_bucket/cbam')
        self.assertEqual(collector.dataset_files.count(), 0)


class CBAMIngestorTest(CBAMIngestorBaseTest):
    """CBAM ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_init(self, mock_get_s3_client_kwargs, mock_get_s3_variables):
        """Test init method."""
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
        }
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            trigger_task=False
        )
        ingestor = CBAMIngestor(session)
        self.assertEqual(ingestor.s3['AWS_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(ingestor.s3_options['key'], 'test_access_key')
        self.assertTrue(ingestor.datasource_file)
        self.assertIn('.zarr', ingestor.datasource_file.name)
        self.assertTrue(ingestor.created)

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_init_with_existing_source(
        self, mock_get_s3_client_kwargs, mock_get_s3_variables
    ):
        """Test init method with existing DataSourceFile."""
        datasource = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='cbam_test.zarr'
        )
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
        }
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            additional_config={
                'datasourcefile_id': datasource.id,
                'datasourcefile_exists': True
            },
            trigger_task=False
        )
        ingestor = CBAMIngestor(session)
        self.assertEqual(ingestor.s3['AWS_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(ingestor.s3_options['key'], 'test_access_key')
        self.assertTrue(ingestor.datasource_file)
        self.assertEqual(ingestor.datasource_file.name, datasource.name)
        self.assertFalse(ingestor.created)

    def test_find_start_latlng(self):
        """Test find start latlng function."""
        metadata = {
            'original_min': -12.5969,
            'min': -27,
            'inc': 0.03574368
        }
        result = find_start_latlng(metadata)
        expected_result = -27.00160304
        self.assertAlmostEqual(result, expected_result, places=6)

    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    def test_is_date_in_zarr(self, mock_open_dataset):
        """Test is_date_in_zarr function."""
        mock_ds = MagicMock()
        mock_ds.date.values = np.array([np.datetime64('2023-01-01')])
        mock_open_dataset.return_value = mock_ds

        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            trigger_task=False
        )
        ingestor = CBAMIngestor(session)
        ingestor.created = False
        ingestor.existing_dates = None
        self.assertTrue(ingestor.is_date_in_zarr(date(2023, 1, 1)))
        self.assertFalse(ingestor.is_date_in_zarr(date(2024, 1, 1)))

    @patch('gap.providers.CBAMNetCDFReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.setup_reader')
    def test_run(
        self, mock_setup_reader, mock_open_dataset, mock_open_dataset_reader
    ):
        """Test run ingestor."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            trigger_task=False
        )
        ingestor = CBAMIngestor(session)
        mock_ds = MagicMock(spec=xrDataset)
        mock_open_dataset.return_value = mock_ds
        mock_open_dataset_reader.return_value = mock_ds

        DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.NETCDF,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min)
        )

        ingestor.is_date_in_zarr = MagicMock(return_value=False)
        ingestor.store_as_zarr = MagicMock()

        ingestor.run()

        ingestor.store_as_zarr.assert_called_once_with(
            mock_ds, date(2023, 1, 1))

    @patch('json.dumps')
    def test_run_success(self, mock_json_dumps):
        """Test successful run."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            trigger_task=False
        )
        ingestor = CBAMIngestor(session)
        ingestor._run = MagicMock()
        ingestor.metadata = {
            'start_date': date(2023, 1, 1),
            'end_date': date(2023, 12, 31),
            'total_processed': 10
        }
        datasourcefile = DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.ZARR,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min)
        )
        ingestor.datasource_file = datasourcefile
        ingestor.run()

        ingestor._run.assert_called_once()
        mock_json_dumps.assert_called_once_with(
            ingestor.metadata, default=str)

    @patch('gap.providers.CBAMNetCDFReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.setup_reader')
    def test_cancel_run(
        self, mock_setup_reader, mock_open_dataset, mock_open_dataset_reader
    ):
        """Test cancel run."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            trigger_task=False
        )
        session.is_cancelled = True
        session.save()
        ingestor = CBAMIngestor(session)
        mock_ds = MagicMock(spec=xrDataset)
        mock_open_dataset.return_value = mock_ds
        mock_open_dataset_reader.return_value = mock_ds

        DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.NETCDF,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min)
        )

        ingestor.is_date_in_zarr = MagicMock(return_value=False)
        ingestor.store_as_zarr = MagicMock()

        ingestor.run()

        ingestor.store_as_zarr.assert_not_called()

    @patch('gap.utils.zarr.BaseZarrReader.get_zarr_base_url')
    @patch('gap.utils.netcdf.find_start_latlng')
    @patch('xarray.core.dataset.Dataset.to_zarr')
    @patch('gap.ingestor.cbam.execute_dask_compute')
    def test_store_as_zarr(
        self, mock_compute, mock_to_zarr, mock_find_start_latlng,
        mock_get_zarr_base_url
    ):
        """Test CBAM store_as_zarr method."""
        dt = date(2023, 1, 1)

        # Create a sample xarray dataset
        lat = np.arange(-12.5969, 16 + 0.03574368, 0.03574368)
        lon = np.arange(26.9665, 52 + 0.036006329, 0.036006329)
        date_range = pd.date_range('2023-01-01', periods=1)

        temperature_data = np.random.rand(len(date_range), len(lat), len(lon))
        dataset = xrDataset(
            {
                'max_total_temperature': (
                    ['date', 'lat', 'lon'],
                    temperature_data
                )
            },
            coords={
                'date': date_range,
                'lat': lat,
                'lon': lon
            }
        )

        # Mock the return values of find_start_latlng
        mock_find_start_latlng.side_effect = [-27, 21.8]

        # Mock zarr base URL
        mock_get_zarr_base_url.return_value = 's3://bucket/'

        # Create instance of CBAMIngestor
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            trigger_task=False
        )
        instance = CBAMIngestor(session=session)
        instance.created = True  # Simulate that Zarr file doesn't exist yet
        instance.s3 = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key'
        }

        # Act
        instance.store_as_zarr(dataset, dt)

        # Assert
        assert 'Date' not in dataset.attrs

        zarr_url = f's3://bucket/{instance.datasource_file.name}'
        mock_to_zarr.assert_called_once_with(
            zarr_url,
            mode='w',
            consolidated=True,
            storage_options=instance.s3_options,
            encoding={
                'date': {'units': 'days since 2023-01-01', 'chunks': 90},
                'max_total_temperature': {'chunks': (90, 300, 300)}
            },
            compute=False,
        )
        mock_compute.assert_called_once()
