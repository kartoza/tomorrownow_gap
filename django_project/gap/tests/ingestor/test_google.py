# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Google Nowcast Ingestor.
"""

import os
import uuid
import numpy as np
import rasterio
from unittest import mock
from parameterized import parameterized
from rasterio.transform import from_bounds
from google.cloud.storage import Bucket, Blob
import xarray as xr
import tempfile
from unittest import TestCase as UTestCase
import shutil
from django.test import TestCase
import ee

from gap.models import (
    CollectorSession,
    IngestorType,
    CollectorSessionProgress,
    IngestorSessionStatus,
    IngestorSession
)
from gap.ingestor.google.collector import GoogleNowcastCollector
# from gap.ingestor.google.ingestor import GoogleNowcastIngestor
from gap.ingestor.google.common import (
    get_forecast_target_time_from_filename
)
from gap.ingestor.google.cog import (
    cog_to_xarray_advanced
)
from gap.tasks.collector import run_google_nowcast_collector_session


def mock_do_nothing(*args, **kwargs):
    """Mock function that does nothing."""
    pass


class TestGoogleIngestorFunction(UTestCase):
    """Test google ingestor functions."""

    @classmethod
    def setUpClass(cls):
        """Set test class for google ingestor functions."""
        cls.working_dir = tempfile.mkdtemp(prefix="nowcast_", suffix="_tmp")

    @classmethod
    def tearDownClass(cls):
        """Remove resources after test class has been run."""
        # Remove the temp directory and its contents
        shutil.rmtree(cls.working_dir)

    @classmethod
    def create_test_cog(
        cls, filename, n_bands=3, width=100, height=100, crs='EPSG:4326',
        nodata=None, band_descriptions=None, dtype=np.float32,
        descending_lat=False, descending_lon=False
    ):
        """Create a test COG file in memory.

        Parameters:
        -----------
        descending_lat : bool
            If True, create file with descending latitude (north to south)
        descending_lon : bool
            If True, create file with descending longitude (east to west)
        """
        # Create test data
        data = np.random.rand(n_bands, height, width).astype(dtype)

        # Add some nodata values if specified
        if nodata is not None:
            # Add nodata values to corners
            data[:, 0:5, 0:5] = nodata
            data[:, -5:, -5:] = nodata

        # Create transform based on coordinate order
        if descending_lat and descending_lon:
            # Both descending
            transform = from_bounds(180, 90, -180, -90, width, height)
        elif descending_lat:
            # Only latitude descending (most common in rasters)
            transform = from_bounds(-180, 90, 180, -90, width, height)
        elif descending_lon:
            # Only longitude descending
            transform = from_bounds(180, -90, -180, 90, width, height)
        else:
            # Both ascending
            transform = from_bounds(-180, -90, 180, 90, width, height)

        # Write the file
        filepath = os.path.join(
            cls.working_dir,
            filename
        )
        with rasterio.open(
            filepath,
            'w',
            driver='GTiff',
            height=height,
            width=width,
            count=n_bands,
            dtype=dtype,
            crs=crs,
            transform=transform,
            nodata=nodata,
            tiled=True,  # Make it cloud-optimized
            compress='lzw'
        ) as dst:
            # Write data
            for i in range(n_bands):
                dst.write(data[i], i + 1)

                # Set band descriptions if provided
                if band_descriptions and i < len(band_descriptions):
                    dst.set_band_description(i + 1, band_descriptions[i])

        return filepath, data

    def test_get_forecast_target_time(self):
        """Tet function get_forecast_target_time_from_filename."""
        filename = 'nowcast_1754434800_40500.tif'
        target_time = get_forecast_target_time_from_filename(
            filename
        )
        self.assertEqual(target_time, 1754475300)
        filename = 'nowcast_1754434800.tif'
        with self.assertRaises(ValueError) as ctx:
            get_forecast_target_time_from_filename(
                filename
            )
        self.assertIn('valid forecast target time', str(ctx.exception))
        filename = 'nowcast_aaa_123.tif'
        with self.assertRaises(ValueError) as ctx:
            get_forecast_target_time_from_filename(
                filename
            )
        self.assertIn('Invalid forecast target time', str(ctx.exception))

    def test_basic_loading(self):
        """Test basic loading of a multi-band COG."""
        filepath, original_data = self.create_test_cog(
            'nowcast_1754434800_0.tif',
            n_bands=3
        )

        # Load with default settings
        ds = cog_to_xarray_advanced(filepath)

        # Check that it's a Dataset
        assert isinstance(ds, xr.Dataset)

        # Check band variables exist
        assert 'band_1' in ds.data_vars
        assert 'band_2' in ds.data_vars
        assert 'band_3' in ds.data_vars

        # Check dimensions
        assert 'lon' in ds.dims
        assert 'lat' in ds.dims

        # Check attributes
        assert 'original_crs' in ds.attrs
        assert 'number_of_bands' in ds.attrs
        assert ds.attrs['number_of_bands'] == 3

    def test_single_band(self):
        """Test loading a single-band COG."""
        filepath, original_data = self.create_test_cog(
            'nowcast_1754434800_200.tif',
            n_bands=1
        )

        ds = cog_to_xarray_advanced(filepath)

        # Should still create a dataset with one variable
        assert isinstance(ds, xr.Dataset)
        assert 'band_1' in ds.data_vars
        assert len(ds.data_vars) == 1

    def test_custom_band_names(self):
        """Test using custom band names."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_300.tif',
            n_bands=4
        )
        custom_names = ['red', 'green', 'blue', 'nir']

        ds = cog_to_xarray_advanced(
            filepath,
            band_names=custom_names,
            verbose=True
        )

        # Check custom names are used
        for name in custom_names:
            assert name in ds.data_vars

        # Check band_names attribute
        assert ds.attrs['band_names'] == custom_names

    def test_band_names_mismatch(self):
        """Test that providing wrong number of band names raises error."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_400.tif',
            n_bands=3
        )

        with self.assertRaises(ValueError) as ctx:
            cog_to_xarray_advanced(
                filepath, band_names=['a', 'b']
            )  # Only 2 names for 3 bands
        self.assertIn("Number of band_names", str(ctx.exception))

    def test_keep_bands_dimension(self):
        """Test keeping bands as a dimension instead of separate variables."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_500.tif',
            n_bands=3
        )

        ds = cog_to_xarray_advanced(filepath, separate_bands=False)

        # Should be DataArray with band dimension
        assert 'band' in ds.dims
        assert len(ds.dims) == 3

        # Should have band coordinate
        assert 'band' in ds.coords

    def test_chunking(self):
        """Test loading with dask chunks."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_600.tif',
            n_bands=2, width=200, height=200
        )

        # Test with dictionary chunks
        ds = cog_to_xarray_advanced(
            filepath,
            chunks={'x': 50, 'y': 50, 'band': 1}
        )

        # Check that data is chunked
        assert ds.attrs.get('chunked')
        assert 'chunk_config' in ds.attrs

        # Check each band is a dask array
        for var in ds.data_vars.values():
            assert hasattr(var.data, 'chunks')

    def test_auto_chunking(self):
        """Test auto chunking."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_700.tif',
            n_bands=2
        )

        ds = cog_to_xarray_advanced(filepath, chunks='auto')

        assert ds.attrs.get('chunked')

    def test_integer_chunking(self):
        """Test single integer for chunk size."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_800.tif',
            n_bands=2
        )

        ds = cog_to_xarray_advanced(filepath, chunks=50)

        assert ds.attrs.get('chunked')

    def test_tuple_chunking(self):
        """Test tuple format for chunks."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_900.tif',
            n_bands=2
        )

        ds = cog_to_xarray_advanced(filepath, chunks=(1, 50, 50))

        assert ds.attrs.get('chunked')

    def test_no_reprojection(self):
        """Test loading without reprojection."""
        # Create file in Web Mercator
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1000.tif',
            n_bands=2, crs='EPSG:3857'
        )

        ds = cog_to_xarray_advanced(filepath, reproject_to_wgs84=False)

        # Should keep original x, y dimensions
        assert 'x' in ds.dims
        assert 'y' in ds.dims
        assert 'lon' not in ds.dims
        assert 'lat' not in ds.dims

        # Check CRS is preserved
        assert 'EPSG:3857' in ds.attrs['original_crs']

    def test_reprojection(self):
        """Test reprojection to WGS84."""
        # Create file in Web Mercator
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1100.tif',
            n_bands=1, crs='EPSG:3857'
        )

        ds = cog_to_xarray_advanced(filepath, reproject_to_wgs84=True)

        # Should have lat/lon dimensions
        assert 'lon' in ds.dims
        assert 'lat' in ds.dims

    def test_nodata_handling(self):
        """Test that nodata values are converted to NaN."""
        nodata_value = -9999
        filepath, original_data = self.create_test_cog(
            'nowcast_1754434800_1200.tif',
            n_bands=2,
            nodata=nodata_value
        )

        ds = cog_to_xarray_advanced(filepath)

        # Check nodata is stored in attributes
        assert ds.attrs['nodata'] == nodata_value

        # Check that original nodata values are now NaN
        band1_data = ds['band_1'].values
        assert np.isnan(band1_data[0, 0, -1])

        # Check middle values are not NaN
        mid_y, mid_x = band1_data.shape[1] // 2, band1_data.shape[2] // 2
        assert not np.isnan(band1_data[0, mid_y, mid_x])

    def test_band_descriptions(self):
        """Test that band descriptions are properly used."""
        descriptions = ['Red Band', 'Green Band', 'Blue Band']
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1300.tif',
            n_bands=3,
            band_descriptions=descriptions
        )

        ds = cog_to_xarray_advanced(filepath)

        # Band descriptions should be cleaned and used as variable names
        assert 'Red_Band' in ds.data_vars
        assert 'Green_Band' in ds.data_vars
        assert 'Blue_Band' in ds.data_vars

    def test_band_metadata_preservation(self):
        """Test that band-specific metadata is preserved."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1400.tif',
            n_bands=2
        )

        ds = cog_to_xarray_advanced(filepath)

        # Check band-specific attributes
        assert ds['band_1'].attrs['band_number'] == 1
        assert ds['band_2'].attrs['band_number'] == 2

    def test_data_integrity(self):
        """Test that data values are preserved correctly."""
        np.random.seed(42)  # For reproducibility
        filepath, original_data = self.create_test_cog(
            'nowcast_1754434800_1500.tif',
            n_bands=2, width=50, height=50
        )

        ds = cog_to_xarray_advanced(filepath)

        # Compare a sample of values (accounting for float precision)
        band1_original = original_data[0]
        band1_loaded = ds['band_1'][0, :, :].values
        # Check shape
        assert band1_loaded.shape == band1_original.shape

        # Check values (with some tolerance for float operations)
        np.testing.assert_allclose(
            band1_loaded[0, 25],
            band1_original[-1, 25],  # sorted
            rtol=1e-5
        )

    def test_large_file_chunking(self):
        """Test that chunk sizes don't exceed file dimensions."""
        # Small file with large chunk request
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1600.tif',
            n_bands=1, width=50, height=50
        )

        # Request chunks larger than file
        ds = cog_to_xarray_advanced(
            filepath,
            chunks={'x': 1000, 'y': 1000, 'band': 10}
        )

        # Should succeed without error
        assert ds is not None
        assert ds.attrs.get('chunked')

    def test_special_characters_in_band_names(self):
        """Test that special characters in band descriptions are handled."""
        descriptions = ['Band-1 (2.0-2.5um)', 'Band@2!', 'Band 3 & 4']
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1700.tif',
            n_bands=3,
            band_descriptions=descriptions
        )

        ds = cog_to_xarray_advanced(filepath)

        # Check that variable names are valid Python identifiers
        for var_name in ds.data_vars:
            # Should only contain alphanumeric and underscores
            assert all(c.isalnum() or c == '_' for c in var_name)

    @parameterized.expand([
        (1, 100, 100),
        (3, 50, 50),
        (10, 200, 200),
    ])
    def test_various_dimensions(self, n_bands, width, height):
        """Test with various file dimensions."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1800.tif',
            n_bands=n_bands, width=width, height=height
        )

        ds = cog_to_xarray_advanced(filepath)

        assert len(ds.data_vars) == n_bands
        assert ds.attrs['number_of_bands'] == n_bands

        # Check dimensions roughly match (exact match depends on reprojection)
        for var in ds.data_vars.values():
            assert len(var.dims) - 1 == 2  # Should have 2 spatial dimensions

    def test_ensure_ascending_coords_default(self):
        """Test that coordinates are sorted to ascending by default."""
        # Create file with descending latitude (common in rasters)
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_1900.tif',
            n_bands=2, descending_lat=True
        )

        # Load with default (ensure_ascending_coords=True)
        ds = cog_to_xarray_advanced(filepath)

        # Check latitude is ascending
        lat_values = ds.lat.values
        assert all(
            lat_values[i] <= lat_values[i + 1] for
            i in range(len(lat_values) - 1)
        ), "Latitude should be in ascending order"

        # Check longitude is ascending
        lon_values = ds.lon.values
        assert all(
            lon_values[i] <= lon_values[i + 1] for
            i in range(len(lon_values) - 1)
        ), "Longitude should be in ascending order"

    def test_ensure_ascending_coords_explicit_true(self):
        """Test explicitly setting ensure_ascending_coords=True."""
        # Create file with both coordinates descending
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_2000.tif',
            n_bands=2,
            descending_lat=True,
            descending_lon=True
        )

        ds = cog_to_xarray_advanced(filepath, ensure_ascending_coords=True)

        # Both should be ascending
        lat_values = ds.lat.values
        lon_values = ds.lon.values

        assert lat_values[0] < lat_values[-1], "Latitude should be ascending"
        assert lon_values[0] < lon_values[-1], "Longitude should be ascending"

    def test_already_ascending_coords(self):
        """Test that already ascending coordinates remain unchanged."""
        # Create file with ascending coordinates
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_2100.tif',
            n_bands=2,
            descending_lat=False,
            descending_lon=False
        )

        ds = cog_to_xarray_advanced(filepath, ensure_ascending_coords=True)

        # Should remain ascending
        lat_values = ds.lat.values
        lon_values = ds.lon.values

        assert all(
            lat_values[i] <= lat_values[i + 1] for
            i in range(len(lat_values) - 1)
        )
        assert all(
            lon_values[i] <= lon_values[i + 1] for
            i in range(len(lon_values) - 1)
        )

    def test_coordinate_sorting_with_chunks(self):
        """Test that coordinate sorting works correctly with chunked data."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_2200.tif',
            n_bands=2,
            width=100,
            height=100,
            descending_lat=True
        )
        ds = cog_to_xarray_advanced(
            filepath,
            chunks={'x': 50, 'y': 50},
            ensure_ascending_coords=True
        )

        # Check that coordinates are ascending
        lat_values = ds.lat.values
        assert all(
            lat_values[i] <= lat_values[i + 1] for
            i in range(len(lat_values) - 1)
        )

        # Check that data is still chunked
        assert ds.attrs.get('chunked')
        for var in ds.data_vars.values():
            assert hasattr(var.data, 'chunks')

    def test_coordinate_sorting_without_reprojection(self):
        """Test coordinate sorting with original CRS (no reprojection)."""
        filepath, _ = self.create_test_cog(
            'nowcast_1754434800_2300.tif',
            n_bands=1,
            crs='EPSG:3857',
            descending_lat=True
        )

        ds = cog_to_xarray_advanced(
            filepath,
            reproject_to_wgs84=False,
            ensure_ascending_coords=True
        )

        # Should have x, y dimensions (not lat/lon)
        assert 'x' in ds.dims
        assert 'y' in ds.dims

        # Y values should be ascending (equivalent to latitude
        # in projected CRS)
        y_values = ds.y.values
        assert all(
            y_values[i] <= y_values[i + 1] for
            i in range(len(y_values) - 1)
        ), "Y coordinates should be ascending"


class TestGoogleNowcastCollector(TestCase):
    """Test Google Nowcast Collector."""

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
        """Init test case."""
        self.init_gee_patcher = mock.patch(
            'gap.ingestor.google.collector.initialize_earth_engine',
            side_effect=mock_do_nothing
        )
        self.mock_init_gee = self.init_gee_patcher.start()
        mock_bucket = mock.MagicMock(spec=Bucket)
        mock_bucket.name = 'nowcast'
        mock_file = mock.MagicMock(spec=Blob)
        mock_file.name = 'nowcast_1755568268_0.tif'
        mock_file.exists.return_value = False
        mock_bucket.blob.return_value = mock_file
        mock_bucket.list_blobs.return_value = [mock_file]
        self.gcs_client_patcher = mock.patch(
            'core.models.object_storage_manager.'
            'ObjectStorageManager.get_gcs_client',
            return_value=mock_bucket
        )
        self.mock_gcs_client = self.gcs_client_patcher.start()
        self.get_latest_timestamp_patcher = mock.patch(
            'gap.ingestor.google.collector.get_latest_nowcast_timestamp',
            return_value=1755568268
        )
        self.mock_get_latest_timestamp = (
            self.get_latest_timestamp_patcher.start()
        )

        mock_task1 = mock.MagicMock(spec=ee.batch.Task)
        mock_task1.active.return_value = False
        mock_task1.status.return_value = {
            'state': 'COMPLETED'
        }
        task1 = {
            'task': mock_task1,
            'timestamp': 1755568268,
            'img_id': '1755568268_0',
            'file_name': 'nowcast_1755568268_0.tif',
            'start_time': None,
            'elapsed_time': None,
            'progress': None,
            'status': None
        }
        self.extract_nowcast_at_timestamp_patcher = mock.patch(
            'gap.ingestor.google.collector.extract_nowcast_at_timestamp',
            return_value=[task1]
        )
        self.mock_extract_nowcast_at_timestamp = (
            self.extract_nowcast_at_timestamp_patcher.start()
        )
        self.working_dir = f'/tmp/{uuid.uuid4().hex}'
        os.makedirs(self.working_dir, exist_ok=True)
        # create tif file nowcast_1755568268_0.tif in the working dir
        with open(
            os.path.join(self.working_dir, 'nowcast_1755568268_0.tif'),
            'wb'
        ) as f:
            f.write(b'test')

    def tearDown(self):
        """Tear down the test case."""
        self.init_gee_patcher.stop()
        self.gcs_client_patcher.stop()
        self.get_latest_timestamp_patcher.stop()
        self.extract_nowcast_at_timestamp_patcher.stop()
        shutil.rmtree(self.working_dir, ignore_errors=True)

    def test_collector_run(self):
        """Test run collector successfully."""
        session = CollectorSession.objects.create(
            ingestor_type=IngestorType.GOOGLE_NOWCAST,
            additional_config={
                'remove_temp_file': True,
                'verbose': True,
            }
        )
        collector = GoogleNowcastCollector(
            session, working_dir=self.working_dir
        )
        collector.run()
        session.refresh_from_db()
        print(session.status)
        print(session.notes)
        progress_list = CollectorSessionProgress.objects.filter(
            collector=session
        )
        print(f'progress count {progress_list.count()}')
        self.mock_init_gee.assert_called_once()
        self.mock_gcs_client.assert_called_once()
        self.mock_get_latest_timestamp.assert_called_once()
        self.mock_extract_nowcast_at_timestamp.assert_called_once()
        self.assertEqual(
            progress_list.count(),
            1
        )
        progress = progress_list.first()
        self.assertEqual(
            progress.status,
            IngestorSessionStatus.SUCCESS
        )
        self.assertEqual(
            session.dataset_files.count(),
            1
        )


class TestGoogleNowcastIngestor(TestCase):
    """Test Google Nowcast Ingestor."""

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

    @mock.patch('gap.models.ingestor.CollectorSession.dataset_files')
    @mock.patch('gap.models.ingestor.CollectorSession.run')
    @mock.patch('gap.tasks.ingestor.run_ingestor_session.apply_async')
    def test_run_nowcast_collector_session(
        self, mock_ingestor, mock_collector, mock_count
    ):
        """Test run nowcast collector session."""
        mock_count.count.return_value = 0
        run_google_nowcast_collector_session()
        # assert
        mock_collector.assert_called_once()
        mock_ingestor.assert_not_called()

        mock_collector.reset_mock()
        mock_ingestor.reset_mock()
        # test with collector result
        mock_count.count.return_value = 1
        run_google_nowcast_collector_session()

        # assert
        session = IngestorSession.objects.filter(
            ingestor_type=IngestorType.GOOGLE_NOWCAST,
        ).order_by('id').last()
        self.assertTrue(session)
        self.assertEqual(session.collectors.count(), 1)
        mock_collector.assert_called_once()
        mock_ingestor.assert_called_once_with(
            args=[session.id],
            queue='high-priority'
        )
        config = session.additional_config
        self.assertFalse(config.get('use_latest_datasource'))
        self.assertNotIn(
            'datasourcefile_id', config
        )
        self.assertNotIn(
            'datasourcefile_exists', config
        )
