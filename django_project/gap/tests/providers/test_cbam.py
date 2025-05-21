# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for CBAM Reader.
"""

from django.test import TestCase
from datetime import datetime
import xarray as xr
import numpy as np
import pandas as pd
from django.contrib.gis.geos import Point, MultiPoint, Polygon, MultiPolygon
from unittest.mock import Mock, patch

from core.settings.utils import absolute_path
from gap.models.dataset import Dataset
from gap.models.measurement import DatasetAttribute
from gap.utils.reader import DatasetReaderInput, LocationInputType
from gap.utils.netcdf import (
    NetCDFProvider,
)
from gap.providers import (
    CBAMNetCDFReader,
    CBAMZarrReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    DataSourceFileFactory
)


class TestCBAMNetCDFReader(TestCase):
    """Unit test for CBAM NetCDFReader class."""

    def setUp(self) -> None:
        """Set test class."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        self.attribute = AttributeFactory.create(
            name='Max Temperature',
            variable_name='max_temperature')
        self.dataset_attr = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute,
            source='max_total_temperature'
        )

    @patch('gap.utils.netcdf.daterange_inc',
           return_value=[datetime(2023, 1, 1)])
    @patch('gap.models.DataSourceFile.objects.filter')
    def test_read_historical_data_empty(
        self, mock_filter, mock_daterange_inc):
        """Test for reading historical data that returns empty."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = CBAMNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.first.return_value = None
        reader.read_historical_data(start_date, end_date)
        self.assertEqual(reader.xrDatasets, [])

    def test_read_historical_data(self):
        """Test for reading historical data from CBAM sample."""
        dt = datetime(2019, 11, 1, 0, 0, 0)
        p = Point(x=26.97, y=-12.56)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'cbam.nc'
        )
        with patch.object(CBAMNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            reader = CBAMNetCDFReader(
                self.dataset, [self.dataset_attr],
                DatasetReaderInput.from_point(p),
                dt, dt)
            reader.read_historical_data(dt, dt)
            mock_open.assert_called_once()
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values().to_json()
            self.assertEqual(len(data_value['data']), 1)
            self.assertAlmostEqual(
                data_value['data'][0]['max_temperature'],
                33.371735, 6)


class TestCBAMZarrReader(TestCase):
    """Test class for CBAMZarrReader."""

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
        """Set test cbam zarr reader."""
        self.dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        self.attribute1 = DatasetAttribute.objects.filter(
            dataset=self.dataset,
            attribute__variable_name='max_temperature'
        ).first()
        self.dt1 = np.datetime64('2024-09-01')
        self.dt2 = np.datetime64('2024-09-01')
        self.xrDataset = xr.Dataset(
            {
                'max_total_temperature': (
                    ('date', 'lat', 'lon'),
                    [
                        [
                            [0.26790932, 0.5398054],
                            [0.74384186, 0.50810691]
                        ]
                    ]
                ),
            },
            coords={
                'date': pd.date_range('2024-09-01', periods=1),
                'lat': [0, 1],
                'lon': [0, 1],
            }
        )
        dt = datetime(2024, 9, 1, 0, 0, 0)
        self.reader = CBAMZarrReader(
            self.dataset, [self.attribute1],
            DatasetReaderInput.from_point(Point(0, 1)),
            dt, dt
        )

    def test_read_from_point(self):
        """Test read cbam data from single point."""
        ds = self.reader._read_variables_by_point(
            self.xrDataset, ['max_total_temperature'],
            self.dt1, self.dt2
        )
        self.assertAlmostEqual(ds['max_total_temperature'], 0.74384186, 2)

    def test_read_from_bbox(self):
        """Test read cbam data from bbox."""
        self.reader.location_input = DatasetReaderInput.from_bbox(
            [0, 0, 1, 1])
        ds = self.reader._read_variables_by_bbox(
            self.xrDataset, ['max_total_temperature'],
            self.dt1, self.dt2
        )
        self.assertTrue(
            np.allclose(
                self.xrDataset['max_total_temperature'].values,
                ds['max_total_temperature'].values
            )
        )

    def test_read_from_polygon(self):
        """Test read cbam data from polygon."""
        self.reader.location_input = DatasetReaderInput(
            MultiPolygon([Polygon.from_bbox((-1, -1, 2, 2))]),
            LocationInputType.POLYGON
        )
        ds = self.reader._read_variables_by_polygon(
            self.xrDataset, ['max_total_temperature'],
            self.dt1, self.dt2
        )
        self.assertTrue(
            np.allclose(
                self.xrDataset['max_total_temperature'].values,
                ds['max_total_temperature'].values
            )
        )

    def test_read_from_points(self):
        """Test read cbam data from list of point."""
        self.reader.location_input = DatasetReaderInput(
            MultiPoint([
                Point(x=0, y=0),
                Point(x=1, y=1)
            ]),
            LocationInputType.LIST_OF_POINT
        )
        ds = self.reader._read_variables_by_points(
            self.xrDataset, ['max_total_temperature'],
            self.dt1, self.dt2
        )
        val = ds['max_total_temperature'].values
        self.assertAlmostEqual(val[0][0][0], 0.26790932)
        self.assertAlmostEqual(val[0][1][1], 0.50810691)
