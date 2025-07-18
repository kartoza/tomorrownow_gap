# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Zarr Utilities.
"""

from unittest import mock
from django.test import TestCase
from datetime import datetime
from xarray.core.dataset import Dataset as xrDataset
from django.contrib import messages
from django.contrib.gis.geos import (
    Point
)
from django.utils import timezone
from unittest.mock import MagicMock, patch

from core.models import ObjectStorageManager
from gap.models import (
    Provider, Dataset, DatasetAttribute,
    DatasetStore
)
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.providers import (
    CBAMZarrReader,
)
from gap.admin.main import (
    load_source_zarr_cache,
    clear_source_zarr_cache,
    clear_zarr_dir_cache,
    calculate_zarr_cache_size
)
from gap.factories import (
    DataSourceFileFactory,
    DataSourceFileCacheFactory
)
from gap.utils.zarr import BaseZarrReader


class TestCBAMZarrReader(TestCase):
    """Unit test for class TestCBAMZarrReader."""

    fixtures = [
        '1.object_storage_manager.json',
    ]

    def setUp(self):
        """Set test for TestCBAMZarrReader."""
        self.provider = Provider(name='CBAM')
        self.dataset = Dataset(provider=self.provider)
        self.attributes = [DatasetAttribute(source='var1'),
                           DatasetAttribute(source='var2')]
        self.location_input = DatasetReaderInput.from_point(
            Point(1.0, 2.0)
        )
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = CBAMZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @patch.dict('os.environ', {
        'GAP_S3_ACCESS_KEY_ID': 'test_access_key',
        'GAP_S3_SECRET_ACCESS_KEY': 'test_secret_key',
        'GAP_S3_ENDPOINT_URL': 'https://test-endpoint.com',
        'GAP_S3_REGION_NAME': '',
        'GAP_S3_PRODUCTS_BUCKET_NAME': 'test-bucket',
        'GAP_S3_PRODUCTS_DIR_PREFIX': 'test-prefix/'
    })
    def test_get_s3_variables(self):
        """Test get_s3_variables function."""
        expected_result = {
            'S3_ACCESS_KEY_ID': 'test_access_key',
            'S3_SECRET_ACCESS_KEY': 'test_secret_key',
            'S3_ENDPOINT_URL': 'https://test-endpoint.com',
            'S3_REGION_NAME': '',
            'S3_BUCKET_NAME': 'test-bucket',
            'S3_DIR_PREFIX': 'test-prefix/',
            'S3_CONNECTION_NAME': 'default'
        }
        result = self.reader.get_s3_variables()
        self.assertEqual(result, expected_result)

    @patch.dict('os.environ', {
        'GAP_S3_ENDPOINT_URL': 'https://test-endpoint.com',
        'GAP_S3_REGION_NAME': 'us-test-1'
    })
    def test_get_s3_client_kwargs(self):
        """Test get_s3_client_kwargs function."""
        # set region name
        conn = ObjectStorageManager.objects.get(connection_name='default')
        conn.region_name = 'GAP_S3_REGION_NAME'
        conn.save()
        expected_result = {
            'endpoint_url': 'https://test-endpoint.com',
            'region_name': 'us-test-1'
        }
        result = self.reader.get_s3_client_kwargs()
        self.assertEqual(result, expected_result)

    def test_get_zarr_base_url(self):
        """Test get_zarr_base_url function."""
        s3 = {
            'S3_DIR_PREFIX': 'test-prefix/',
            'S3_BUCKET_NAME': 'test-bucket'
        }
        expected_result = 's3://test-bucket/test-prefix/'
        result = self.reader.get_zarr_base_url(s3)
        self.assertEqual(result, expected_result)

    @patch.dict('os.environ', {
        'GAP_S3_ACCESS_KEY_ID': 'test_access_key',
        'GAP_S3_SECRET_ACCESS_KEY': 'test_secret_key',
        'GAP_S3_ENDPOINT_URL': 'https://test-endpoint.com',
        'GAP_S3_REGION_NAME': 'us-test-1',
        'GAP_S3_PRODUCTS_BUCKET_NAME': 'test-bucket',
        'GAP_S3_PRODUCTS_DIR_PREFIX': 'test-prefix/'
    })
    @patch('xarray.open_zarr')
    @patch('fsspec.filesystem')
    @patch('s3fs.S3FileSystem')
    @patch('os.uname')
    @patch('os.getpid')
    def test_open_dataset(
        self, mock_getpid, mock_uname, mock_s3fs,
        mock_fsspec_filesystem, mock_open_zarr
    ):
        """Test open zarr dataset function."""
        # Mock uname to get hostname
        mock_uname.return_value = [0, 'test-host']
        mock_getpid.return_value = 1234

        # Mock the s3fs.S3FileSystem constructor
        mock_s3fs_instance = MagicMock()
        mock_s3fs.return_value = mock_s3fs_instance

        # Mock the fsspec.filesystem constructor
        mock_fs_instance = MagicMock()
        mock_fsspec_filesystem.return_value = mock_fs_instance

        # Mock the xr.open_zarr function
        mock_dataset = MagicMock(spec=xrDataset)
        mock_open_zarr.return_value = mock_dataset

        source_file = DataSourceFileFactory.create(
            name='test_dataset.zarr',
            metadata={
                'drop_variables': ['test']
            }
        )
        self.reader.setup_reader()
        result = self.reader.open_dataset(source_file)

        # Assertions to ensure the method is called correctly
        assert result == mock_dataset
        mock_s3fs.assert_called_once_with(
            key='test_access_key',
            secret='test_secret_key',
            client_kwargs={
                'endpoint_url': 'https://test-endpoint.com'
            }
        )
        mock_fsspec_filesystem.assert_called_once_with(
            'filecache',
            target_protocol='s3',
            target_options=self.reader.s3_options,
            cache_storage=f'/tmp/test-host_1234_{source_file.id}',
            cache_check=3600,
            expiry_time=86400,
            target_kwargs={'s3': mock_s3fs_instance}
        )
        mock_fs_instance.get_mapper.assert_called_once_with(
            's3://test-bucket/test-prefix/test_dataset.zarr')
        mock_open_zarr.assert_called_once_with(
            mock_fs_instance.get_mapper.return_value,
            consolidated=True, drop_variables=['test'])

    @patch.dict('os.environ', {
        'GAP_S3_ACCESS_KEY_ID': 'test_access_key',
        'GAP_S3_SECRET_ACCESS_KEY': 'test_secret_key',
        'GAP_S3_ENDPOINT_URL': 'https://test-endpoint.com',
        'GAP_S3_REGION_NAME': 'us-test-1',
        'GAP_S3_PRODUCTS_BUCKET_NAME': 'test-bucket',
        'GAP_S3_PRODUCTS_DIR_PREFIX': 'test-prefix/'
    })
    @patch('xarray.open_zarr')
    @patch('fsspec.filesystem')
    @patch('s3fs.S3FileSystem')
    @patch('os.uname')
    @patch('os.getpid')
    def test_open_dataset_with_cache(
        self, mock_getpid, mock_uname, mock_s3fs, mock_fsspec_filesystem,
        mock_open_zarr
    ):
        """Test open zarr dataset function."""
        # Mock the s3fs.S3FileSystem constructor
        mock_s3fs_instance = MagicMock()
        mock_s3fs.return_value = mock_s3fs_instance

        # Mock the fsspec.filesystem constructor
        mock_fs_instance = MagicMock()
        mock_fsspec_filesystem.return_value = mock_fs_instance

        # Mock the xr.open_zarr function
        mock_dataset = MagicMock(spec=xrDataset)
        mock_open_zarr.return_value = mock_dataset

        # Mock uname to get hostname
        mock_uname.return_value = [0, 'test-host']
        mock_getpid.return_value = 1234

        source_file = DataSourceFileFactory.create(
            name='test_dataset.zarr',
            metadata={
                'drop_variables': ['test']
            }
        )
        source_file_cache = DataSourceFileCacheFactory.create(
            source_file=source_file,
            hostname='test-host_1234',
            expired_on=timezone.now()
        )
        self.reader.setup_reader()
        result = self.reader.open_dataset(source_file)

        # Assertions to ensure the method is called correctly
        assert result == mock_dataset
        mock_s3fs.assert_called_once_with(
            key='test_access_key',
            secret='test_secret_key',
            client_kwargs={
                'endpoint_url': 'https://test-endpoint.com'
            }
        )
        cache_filename = f'test-host_1234_{source_file.id}'
        mock_fsspec_filesystem.assert_called_once_with(
            'filecache',
            target_protocol='s3',
            target_options=self.reader.s3_options,
            cache_storage=f'/tmp/{cache_filename}',
            cache_check=3600,
            expiry_time=86400,
            target_kwargs={'s3': mock_s3fs_instance}
        )
        mock_fs_instance.get_mapper.assert_called_once_with(
            's3://test-bucket/test-prefix/test_dataset.zarr')
        mock_open_zarr.assert_called_once_with(
            mock_fs_instance.get_mapper.return_value,
            consolidated=True, drop_variables=['test'])
        mock_uname.assert_called()
        # assert cache does not expired_on
        source_file_cache.refresh_from_db()
        self.assertIsNone(source_file_cache.expired_on)

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_setup_reader(
        self, mock_get_s3_client_kwargs, mock_get_s3_variables
    ):
        """Test setup_reader function."""
        mock_get_s3_variables.return_value = {
            'S3_ACCESS_KEY_ID': 'test_access_key',
            'S3_SECRET_ACCESS_KEY': 'test_secret_key',
            'S3_ENDPOINT_URL': 'https://test-endpoint.com',
            'S3_REGION_NAME': 'us-test-1'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
        }
        self.reader.setup_reader()

        self.assertEqual(
            self.reader.s3['S3_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(self.reader.s3_options['key'], 'test_access_key')
        self.assertEqual(self.reader.s3_options['secret'], 'test_secret_key')
        self.assertEqual(
            self.reader.s3_options['client_kwargs']['endpoint_url'],
            'https://test-endpoint.com'
        )


class TestAdminZarrFileActions(TestCase):
    """Test for actions for Zarr DataSourceFile."""

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

    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.setup_reader')
    def test_load_source_zarr_cache(
        self, mock_setup_reader, mock_open_dataset
    ):
        """Test load cache for DataSourceFile zarr."""
        # Mock the queryset with a Zarr file
        data_source = DataSourceFileFactory.create(
            format=DatasetStore.ZARR,
            name='test.zarr'
        )
        mock_queryset = [data_source]

        # Mock the modeladmin and request objects
        mock_modeladmin = MagicMock()
        mock_request = MagicMock()

        # Call the load_source_zarr_cache function
        load_source_zarr_cache(mock_modeladmin, mock_request, mock_queryset)

        # Assertions
        mock_setup_reader.assert_called_once()
        mock_open_dataset.assert_called_once_with(data_source)
        mock_modeladmin.message_user.assert_called_once_with(
            mock_request, 'test.zarr zarr cache has been loaded!',
            messages.SUCCESS
        )

    @patch('shutil.rmtree')
    @patch('os.path.exists')
    @patch('os.uname')
    @patch('os.getpid')
    def test_clear_source_zarr_cache(
        self, mock_getpid, mock_uname, mock_os_path_exists, mock_rmtree
    ):
        """Test clear cache DataSourceFile zarr."""
        # Mock uname to get hostname
        mock_uname.return_value = [0, 'test-host']
        mock_getpid.return_value = 1234

        # Mock the queryset with a Zarr file
        data_source = DataSourceFileFactory.create(
            format=DatasetStore.ZARR,
            name='test.zarr'
        )
        mock_queryset = [data_source]

        # Mock os.path.exists to return True
        mock_os_path_exists.return_value = True

        # Mock the modeladmin and request objects
        mock_modeladmin = MagicMock()
        mock_request = MagicMock()

        # Call the clear_source_zarr_cache function
        clear_source_zarr_cache(mock_modeladmin, mock_request, mock_queryset)

        # Assertions
        cache_dir = f'/tmp/test-host_1234_{data_source.id}'
        mock_os_path_exists.assert_called_once_with(cache_dir)
        mock_rmtree.assert_called_once_with(cache_dir)
        mock_modeladmin.message_user.assert_called_once_with(
            mock_request,
            f'{cache_dir} has been cleared!',
            messages.SUCCESS
        )

    @patch('gap.admin.main.get_directory_size')
    @patch('os.path.exists')
    @patch('os.uname')
    @patch('os.getpid')
    def test_calculate_zarr_cache_size(
        self, mock_getpid, mock_uname, mock_os_path_exists, mock_calculate
    ):
        """Test calculate zarr cache size."""
        # Mock uname to get hostname
        mock_uname.return_value = [0, 'test-host']
        mock_getpid.return_value = 1234
        # Mock the queryset with a Zarr file
        data_source = DataSourceFileFactory.create(
            format=DatasetStore.ZARR,
            name='test.zarr'
        )
        cache_file = DataSourceFileCacheFactory.create(
            source_file=data_source
        )

        mock_queryset = [cache_file]

        # Mock os.path.exists to return True
        mock_os_path_exists.return_value = True
        # Mock get_directory_size to return 10000
        mock_calculate.return_value = 10000

        # Mock the modeladmin and request objects
        mock_modeladmin = MagicMock()
        mock_request = MagicMock()

        calculate_zarr_cache_size(mock_modeladmin, mock_request, mock_queryset)

        # Assertions
        cache_dir = f'/tmp/test-host_1234_{data_source.id}'
        mock_os_path_exists.assert_called_once_with(cache_dir)
        mock_calculate.assert_called_once_with(cache_dir)
        mock_modeladmin.message_user.assert_called_once_with(
            mock_request,
            'Calculate zarr cache size successful!',
            messages.SUCCESS
        )
        cache_file.refresh_from_db()
        self.assertEqual(cache_file.size, 10000)

    @patch('shutil.rmtree')
    @patch('os.path.exists')
    @patch('os.uname')
    @patch('os.getpid')
    def test_clear_zarr_dir_cache(
        self, mock_getpid, mock_uname, mock_os_path_exists, mock_rmtree
    ):
        """Test clear_zarr_dir_cache."""
        # Mock uname to get hostname
        mock_uname.return_value = [0, 'test-host']
        mock_getpid.return_value = 1234
        # Mock the queryset with a Zarr file
        data_source = DataSourceFileFactory.create(
            format=DatasetStore.ZARR,
            name='test.zarr'
        )
        cache_file = DataSourceFileCacheFactory.create(
            source_file=data_source
        )

        mock_queryset = [cache_file]

        # Mock os.path.exists to return True
        mock_os_path_exists.return_value = True

        # Mock the modeladmin and request objects
        mock_modeladmin = MagicMock()
        mock_request = MagicMock()

        # Call the clear_zarr_dir_cache function
        clear_zarr_dir_cache(mock_modeladmin, mock_request, mock_queryset)

        # Assertions
        cache_dir = f'/tmp/test-host_1234_{data_source.id}'
        mock_os_path_exists.assert_called_once_with(cache_dir)
        mock_rmtree.assert_called_once_with(cache_dir)
        mock_modeladmin.message_user.assert_called_once_with(
            mock_request,
            f'{cache_dir} has been cleared!',
            messages.SUCCESS
        )


class TestBaseZarrReader(TestCase):
    """Test for BaseZarrReader utility functions."""

    @mock.patch('os.getpid')
    @mock.patch('os.uname')
    def test_get_zarr_cache_dir(self, mock_uname, mock_getpid):
        """Test get_zarr_cache_dir function."""
        mock_uname.return_value = ['test-host', 'test-hostname']
        mock_getpid.return_value = 1234
        data_source = DataSourceFileFactory.create(
            name='test.zarr'
        )
        expected_cache_dir = f'/tmp/test-hostname_1234_{data_source.id}'
        cache_dir = BaseZarrReader.get_zarr_cache_dir(data_source)
        self.assertEqual(cache_dir, expected_cache_dir)
