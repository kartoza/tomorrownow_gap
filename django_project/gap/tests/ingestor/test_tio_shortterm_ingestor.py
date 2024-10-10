# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tio Shortterm Ingestor.
"""

import os
from unittest.mock import patch, MagicMock
from datetime import date
import numpy as np
import pandas as pd
import dask.array as da
from xarray.core.dataset import Dataset as xrDataset
from django.test import TestCase
from django.contrib.gis.geos import Polygon

from gap.models import Dataset, DatasetStore
from gap.models.ingestor import (
    IngestorSession,
    IngestorType,
    CollectorSession
)
from gap.ingestor.tio_shortterm import TioShortTermIngestor, CoordMapping
from gap.ingestor.exceptions import (
    MissingCollectorSessionException, FileNotFoundException,
    AdditionalConfigNotFoundException
)
from gap.factories import DataSourceFileFactory, GridFactory


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


def create_polygon():
    """Create mock polygon for Grid."""
    return Polygon.from_bbox([
        LON_METADATA['min'],
        LAT_METADATA['min'],
        LON_METADATA['min'] + 2 * LON_METADATA['inc'],
        LAT_METADATA['min'] + 2 * LON_METADATA['inc'],
    ])


class TestTioIngestor(TestCase):
    """Salient ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    @patch('gap.utils.netcdf.NetCDFMediaS3')
    @patch('gap.utils.zarr.BaseZarrReader')
    def setUp(
            self, mock_zarr_reader,
            mock_netcdf_media_s3):
        """Initialize TestSalientIngestor."""
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
            name='tio.zarr'
        )
        self.session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            trigger_task=False,
            additional_config={
                'datasourcefile_id': self.zarr_source.id,
                'datasourcefile_zarr_exists': True
            }
        )
        self.session.collectors.set([self.collector])

        self.mock_zarr_reader = mock_zarr_reader
        self.mock_netcdf_media_s3 = mock_netcdf_media_s3
        # self.mock_dask_compute = mock_dask_compute

        self.ingestor = TioShortTermIngestor(self.session)
        self.ingestor.lat_metadata = LAT_METADATA
        self.ingestor.lon_metadata = LON_METADATA

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_init_with_existing_source(
        self, mock_get_s3_client_kwargs, mock_get_s3_variables
    ):
        """Test init method with existing DataSourceFile."""
        datasource = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='tio_test.zarr'
        )
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
        }
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            additional_config={
                'datasourcefile_id': datasource.id,
                'datasourcefile_zarr_exists': True,
                'remove_temp_file': False
            },
            trigger_task=False
        )
        ingestor = TioShortTermIngestor(session)
        self.assertEqual(ingestor.s3['AWS_ACCESS_KEY_ID'], 'test_access_key')
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

    @patch('gap.ingestor.tio_shortterm.execute_dask_compute')
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

    @patch('gap.ingestor.tio_shortterm.execute_dask_compute')
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
    @patch('gap.ingestor.tio_shortterm.execute_dask_compute')
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
        mock_default_storage.return_value = open(filepath, "rb")

        # create a grid
        grid = GridFactory(geometry=create_polygon())
        # mock zip_file.namelist
        mock_namelist.return_value = [f'grid-{grid.id}.json']
        with patch.object(self.ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.ingestor._run()

        mock_default_storage.assert_called_once()
        mock_dask_compute.assert_called_once()
        self.assertEqual(self.ingestor.metadata['total_json_processed'], 1)
        self.assertEqual(len(self.ingestor.metadata['chunks']), 1)

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
