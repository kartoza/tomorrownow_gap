# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for CBAM Bias Adjust Ingestor.
"""

from unittest.mock import patch, MagicMock
from datetime import datetime, date, time
import numpy as np
import pandas as pd
import dask.array as da
from xarray.core.dataset import Dataset as xrDataset
from django.test import TestCase

from gap.models import Dataset, DataSourceFile, DatasetStore
from gap.models.ingestor import (
    IngestorSession,
    IngestorType,
    CollectorSession,
    IngestorSessionStatus
)
from gap.ingestor.cbam_bias_adjust import CBAMBiasAdjustIngestor
from gap.factories import DataSourceFileFactory
from gap.ingestor.base import CoordMapping


LAT_METADATA = {
    'min': -12.5969,
    'max': -12.525413,
    'inc': 0.03574368,
    'original_min': -12.5969
}
LON_METADATA = {
    'min': 26.9665,
    'max': 27.038513,
    'inc': 0.036006329,
    'original_min': 26.9665
}


def mock_open_zarr_dataset(date_var = 'date'):
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
    date_array = pd.date_range(
        '2023-01-01', periods=1)
    empty_shape = (1, len(new_lat), len(new_lon))
    chunks = (1, 100, 100)
    data_vars = {
        'max_total_temperature': (
            [date_var, 'lat', 'lon'],
            da.empty(empty_shape, chunks=chunks)
        )
    }
    return xrDataset(
        data_vars=data_vars,
        coords={
            date_var: (date_var, date_array),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }
    )


class CBAMBiasAdjustIngestorBaseTest(TestCase):
    """Base test for CBAM ingestor/collector."""

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
        """Set CBAMIngestorBaseTest."""
        self.dataset = Dataset.objects.get(
            name='CBAM Climate Reanalysis (Bias-Corrected)')


class CBAMBiasAdjustCollectorTest(CBAMBiasAdjustIngestorBaseTest):
    """CBAM collector test case."""

    @patch(
        'core.models.object_storage_manager.ObjectStorageManager.'
        'get_s3_env_vars'
    )
    @patch('gap.ingestor.cbam_bias_adjust.s3fs.S3FileSystem')
    def test_cbam_collector(self, mock_s3fs, mock_get_s3_env):
        """Test run cbam collector."""
        mock_get_s3_env.return_value = {
            'S3_DIR_PREFIX': 'cbam',
            'S3_ENDPOINT_URL': 'test_endpoint',
            'S3_BUCKET_NAME': 'test_bucket'
        }
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.walk.return_value = [
            (
                'test_bucket/cbam_bias/max_total_temperature', [],
                ['max_total_temperature_interpolated_2012_RBF.nc']
            ),
            (
                'test_bucket/cbam_bias/min_total_temperature', [],
                ['min_total_temperature_interpolated_2012_RBF.nc']
            ),
            ('test_bucket/cbam_bias/dmrpp', [], ['2023-01-01.nc.dmrpp']),
            (
                'test_bucket/cbam_bias/total_rainfall', [],
                ['total_rainfall_interpolated_2012_RBF.nc']
            ),
        ]
        # add existing NetCDF File
        DataSourceFileFactory.create(
            dataset=self.dataset,
            name='total_rainfall/total_rainfall_interpolated_2012_RBF.nc'
        )
        collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            additional_config={
                'directory_path': 'cbam_bias'
            }
        )
        collector.run()
        collector.refresh_from_db()
        self.assertEqual(collector.status, IngestorSessionStatus.SUCCESS)
        mock_fs.walk.assert_called_with('s3://test_bucket/cbam_bias')
        self.assertEqual(
            DataSourceFile.objects.filter(
                dataset=self.dataset,
                name='total_rainfall/total_rainfall_interpolated_2012_RBF.nc'
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
                dataset=self.dataset,
                name=(
                    'max_total_temperature/'
                    'max_total_temperature_interpolated_2012_RBF.nc'
                )
            ).exists()
        )
        self.assertTrue(
            DataSourceFile.objects.filter(
                dataset=self.dataset,
                name=(
                    'min_total_temperature/'
                    'min_total_temperature_interpolated_2012_RBF.nc'
                )
            ).exists()
        )



class CBAMBiasAdjustIngestorTest(CBAMBiasAdjustIngestorBaseTest):
    """CBAM ingestor test case."""

    @patch('json.dumps')
    def test_run_success(self, mock_json_dumps):
        """Test successful run."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            trigger_task=False
        )
        ingestor = CBAMBiasAdjustIngestor(session)
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

    @patch('gap.ingestor.cbam_bias_adjust.execute_dask_compute')
    def test_append_new_date(self, mock_dask_compute):
        """Test append new date method."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            trigger_task=False
        )
        ingestor = CBAMBiasAdjustIngestor(session)
        date1 = date(2024, 10, 1)
        ingestor._append_new_date(date1, True)
        mock_dask_compute.assert_called_once()

        mock_dask_compute.reset_mock()
        date2 = date(2024, 10, 2)
        ingestor._append_new_date(date2, False)
        mock_dask_compute.assert_called_once()

    def test_is_date_in_zarr(self):
        """Test check date in zarr function."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            trigger_task=False
        )
        ingestor = CBAMBiasAdjustIngestor(session)
        ingestor.created = False
        with patch.object(ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.assertTrue(ingestor._is_date_in_zarr(date(2023, 1, 1)))
            mock_open.assert_called_once()
            mock_open.reset_mock()
            self.assertFalse(ingestor._is_date_in_zarr(date(2024, 10, 1)))
            mock_open.assert_not_called()
            # created is True
            ingestor.created = True
            self.assertFalse(ingestor._is_date_in_zarr(date(2024, 10, 2)))
            ingestor.created = False

    @patch('xarray.open_dataset')
    @patch('s3fs.S3FileSystem')
    def test_open_netcdf_file(
        self, mock_s3_filesystem, mock_open_dataset
    ):
        """Test open_netcdf_file."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            trigger_task=False
        )
        ingestor = CBAMBiasAdjustIngestor(session)
        ingestor.created = False
        # Set up mocks
        mock_open_dataset.return_value = MagicMock(spec=xrDataset)

        # raise exception empty directory_path
        datasourcefile = DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.ZARR,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min),
            metadata={
                'attribute': 'max_total_temperature',
                's3_connection_name': 'default'
            }
        )
        with self.assertRaises(RuntimeError) as ctx:
            ingestor._open_netcdf_file(datasourcefile)
        self.assertIn('directory_path', str(ctx.exception))

        # raise exception empty attribute
        datasourcefile = DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.ZARR,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min),
            metadata={
                'directory_path': 'cbam_bias',
                's3_connection_name': 'default'
            }
        )
        with self.assertRaises(RuntimeError) as ctx:
            ingestor._open_netcdf_file(datasourcefile)
        self.assertIn('attribute', str(ctx.exception))

        # test success
        datasourcefile = DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.ZARR,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min),
            metadata={
                'directory_path': 'cbam_bias',
                'attribute': 'max_total_temperature',
                's3_connection_name': 'default'
            }
        )
        ingestor._open_netcdf_file(datasourcefile)

        # Assertions
        mock_open_dataset.assert_called_once()
        self.assertTrue(mock_s3_filesystem.called)

    @patch('gap.ingestor.cbam_bias_adjust.execute_dask_compute')
    def test_write_by_region(self, mock_dask_compute):
        """Test write_by_region."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            trigger_task=False
        )
        ingestor = CBAMBiasAdjustIngestor(session)
        ingestor.created = False

        new_data = {
            'max_total_temperature': np.random.rand(1, 1, 1)
        }
        date_arr = [CoordMapping(
            date(2023, 1, 1), 0, date(2023, 1, 1)
        )]
        lat_arr = [CoordMapping(
            LAT_METADATA['min'], 0, LAT_METADATA['min']
        )]
        lon_arr = [CoordMapping(
            LON_METADATA['min'], 0, LON_METADATA['min']
        )]
        ingestor._write_by_region(
            date_arr, lat_arr, lon_arr, new_data)
        mock_dask_compute.assert_called_once()

    @patch('xarray.Dataset.to_zarr')
    @patch('xarray.open_dataset')
    @patch('s3fs.S3FileSystem')
    @patch('gap.ingestor.cbam_bias_adjust.execute_dask_compute')
    def test_run_ingestor(
        self, mock_dask_compute, mock_s3_filesystem,
        mock_open_dataset, mock_to_zarr
    ):
        """Test ingestor _run method."""
        mock_open_dataset.return_value = mock_open_zarr_dataset('time')

        data_source = DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.NETCDF,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min),
            metadata={
                'directory_path': 'cbam_bias',
                'attribute': 'max_total_temperature',
                's3_connection_name': 'default'
            }
        )
        collector = CollectorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            additional_config={
                'directory_path': 'cbam_bias'
            }
        )
        collector.dataset_files.set([data_source])

        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM_BIAS_ADJUST,
            trigger_task=False
        )
        session.collectors.set([collector])
        ingestor = CBAMBiasAdjustIngestor(session)
        ingestor.created = False

        with patch.object(ingestor, '_open_zarr_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            ingestor._run()

        # Assertions
        self.assertEqual(ingestor.metadata['total_processed'], 1)
        mock_to_zarr.assert_called_once()
        mock_dask_compute.assert_called_once()
