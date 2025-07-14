# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Ingestor.
"""
import os

from django.contrib.gis.geos import Point, MultiPolygon, Polygon
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.factories.farm import FarmFactory
from gap.ingestor.exceptions import FileNotFoundException
from gap.ingestor.grid import Keys
from gap.models.grid import Grid
from gap.models.ingestor import (
    IngestorType, IngestorSession, IngestorSessionStatus
)
from gap.models.station import Country


class GridIngestorTest(TestCase):
    """Grid ingestor test case."""

    fixtures = [
        '1.object_storage_manager.json',
    ]

    def setUp(self):
        """Init test case."""
        self.country = Country.objects.create(
            name='Country 1',
            iso_a3='COUNTRY_1',
            geometry=MultiPolygon(
                [Polygon(((0, 0), (0, 30), (30, 30), (30, 0), (0, 0)))]
            )
        )
        FarmFactory(
            unique_id='farm-1',
            geometry=Point(0, 0)
        )
        FarmFactory(
            unique_id='farm-2',
            geometry=Point(11, 11)
        )
        FarmFactory(
            unique_id='farm-3',
            geometry=Point(21, 21)
        )
        FarmFactory(
            unique_id='farm-4',
            geometry=Point(12, 12)
        )
        FarmFactory(
            unique_id='farm-5',
            geometry=Point(22, 22)
        )

    def test_no_file(self):
        """Test no file ingestor."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.GRID,
            trigger_task=False
        )
        session.run()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_file(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'excel.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID,
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, "File should be csv"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_column(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'error_incorrect_header.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID,
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes,
            f"Row 2 does not have '{Keys.UNIQUE_ID}'"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_wkt(self):
        """Test when ingestor error wkt."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'error_wkt.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID,
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes,
            "Row 2 : wkt is not correct"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_correct(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'correct.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID,
            trigger_task=False
        )
        session.run()
        session.delete()
        self.assertEqual(session.notes, '3/3')
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)

        grids = Grid.objects.order_by('unique_id')
        self.assertEqual(grids.count(), 3)
        grids = [
            grids.get(unique_id='A0001'), grids.get(unique_id='A0002'),
            grids.get(unique_id='A0003')
        ]

        for grid in grids:
            self.assertEqual(grid.country.name, 'Country 1')

        grid = grids[0]
        self.assertEqual(grid.unique_id, 'A0001')
        self.assertEqual(
            [grid.geometry.centroid.y, grid.geometry.centroid.x],
            [5, 5]
        )
        grid = grids[1]
        self.assertEqual(grid.unique_id, 'A0002')
        self.assertEqual(
            [grid.geometry.centroid.y, grid.geometry.centroid.x],
            [15, 15]
        )
        grid = grids[2]
        self.assertEqual(grid.unique_id, 'A0003')
        self.assertEqual(
            [grid.geometry.centroid.y, grid.geometry.centroid.x],
            [25, 25]
        )

    def test_correct_use_country_id(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'correct.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID,
            trigger_task=False,
            additional_config={
                'country_id': self.country.id
            }
        )
        session.run()
        session.delete()
        self.assertEqual(session.notes, '3/3')
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)

        grids = Grid.objects.order_by('unique_id')
        self.assertEqual(grids.count(), 3)
        grids = [
            grids.get(unique_id='A0001'), grids.get(unique_id='A0002'),
            grids.get(unique_id='A0003')
        ]

        for grid in grids:
            self.assertEqual(grid.country.name, 'Country 1')
