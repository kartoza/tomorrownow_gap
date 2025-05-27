# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tamsat Reader.
"""

from django.test import TestCase
from datetime import datetime
import pytz
import xarray as xr
import numpy as np
import pandas as pd
import dask.array as da
from django.contrib.gis.geos import Point, MultiPoint, Polygon, MultiPolygon
from xarray.core.dataset import Dataset as xrDataset
from unittest.mock import patch
from io import StringIO

from gap.models import (
    DatasetAttribute, Dataset, DatasetStore,
    Attribute
)
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType,
    DatasetReaderValue
)
from gap.providers.tamsat import TamsatZarrReader
from gap.factories import (
    DataSourceFileFactory
)


LAT_METADATA = {
    'min': -18.375,
    'max': -17.475,
    'inc': 0.0375,
    'original_min': -17.475
}
LON_METADATA = {
    'min': 21.7875,
    'max': 22.2,
    'inc': 0.0375,
    'original_min': 21.7875
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
    dayofyear = np.arange(1, 15, 1)
    empty_shape = (len(dayofyear), len(new_lat), len(new_lon))
    chunks = (len(dayofyear), 150, 110)
    data_vars = {
        'rfe_filled': (
            ['dayofyear', 'lat', 'lon'],
            da.empty(empty_shape, chunks=chunks)
        )
    }
    return xrDataset(
        data_vars=data_vars,
        coords={
            'dayofyear': ('dayofyear', dayofyear),
            'lat': ('lat', new_lat),
            'lon': ('lon', new_lon)
        }
    )


class TestTamsatZarrReader(TestCase):
    """Unit test for Tamsat Zarr Reader class."""

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
        """Set Test class for Tamsat Zarr Reader."""
        self.dataset = Dataset.objects.get(
            name='Tamsat Long Term Normals (10 years)',
            store_type=DatasetStore.ZARR
        )
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='tamsat.zarr',
            is_latest=True
        )
        self.attribute1 = Attribute.objects.get(
            name='Precipitation Total',
            variable_name='precipitation_total')
        self.dataset_attr1 = DatasetAttribute.objects.get(
            dataset=self.dataset,
            attribute=self.attribute1,
            source='rfe_filled'
        )
        self.attributes = [self.dataset_attr1]
        self.location_input = DatasetReaderInput.from_point(
            Point(LON_METADATA['min'], LAT_METADATA['min'])
        )
        self.start_date = datetime(2025, 1, 1)
        self.end_date = datetime(2025, 1, 10)
        self.reader = TamsatZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    def test_read_from_bbox(self):
        """Test for reading LTN data using bbox."""
        dt1 = datetime(2025, 1, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2025, 1, 10, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.location_input = DatasetReaderInput.from_bbox(
                [
                    LON_METADATA['min'],
                    LAT_METADATA['min'],
                    LON_METADATA['min'] + LON_METADATA['inc'],
                    LAT_METADATA['min'] + LAT_METADATA['inc']
                ]
            )
            self.reader.read_historical_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('precipitation_total', dataset.data_vars)
            result = list(data_value.to_csv_stream())
            header = result[0].decode('utf-8')
            self.assertIn('date', header)
            self.assertIn('lat', header)
            self.assertIn('lon', header)
            self.assertIn('precipitation_total', header)
            result_str = StringIO(header + '\n' + str(result[1]))
            result_df = pd.read_csv(result_str)
            df = result_df[
                (result_df['date'] == '01-01')
            ]
            self.assertEqual(len(df), 4)

    def test_read_from_points(self):
        """Test for reading LTN data using points."""
        dt1 = datetime(2025, 1, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2025, 1, 10, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            p1 = Point(LON_METADATA['min'], LAT_METADATA['min'])
            p2 = Point(
                LON_METADATA['min'] + LON_METADATA['inc'],
                LAT_METADATA['min'] + LAT_METADATA['inc']
            )
            self.reader.location_input = DatasetReaderInput(
                MultiPoint([p1, p2]),
                LocationInputType.LIST_OF_POINT
            )
            self.reader.read_historical_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('precipitation_total', dataset.data_vars)

    def test_read_from_point(self):
        """Test for reading LTN data using points."""
        dt1 = datetime(2025, 1, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2025, 1, 10, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            p1 = Point(LON_METADATA['min'], LAT_METADATA['min'])
            self.reader.location_input = DatasetReaderInput.from_point(p1)
            self.reader.read_historical_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('precipitation_total', dataset.data_vars)

    def test_read_from_polygon(self):
        """Test for reading LTN data using polygon."""
        dt1 = datetime(2025, 1, 1, tzinfo=pytz.UTC)
        dt2 = datetime(2025, 1, 10, tzinfo=pytz.UTC)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.location_input = DatasetReaderInput(
                MultiPolygon([Polygon.from_bbox((
                    LON_METADATA['min'],
                    LAT_METADATA['min'],
                    LON_METADATA['min'] + LON_METADATA['inc'],
                    LAT_METADATA['min'] + LAT_METADATA['inc']
                ))]),
                LocationInputType.POLYGON
            )
            self.reader.read_historical_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('precipitation_total', dataset.data_vars)
