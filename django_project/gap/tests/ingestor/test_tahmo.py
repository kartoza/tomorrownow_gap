# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Ingestor.
"""
import os

from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import GEOSGeometry
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.ingestor.exceptions import FileNotFoundException
from gap.models.dataset import Dataset
from gap.models.measurement import DatasetAttribute, Measurement
from gap.models.ingestor import (
    IngestorSession, IngestorSessionStatus, IngestorType
)
from gap.models.station import Station, Country
from gap.factories import MeasurementFactory, StationFactory


class TahmoIngestorTest(TestCase):
    """Tahmo ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.TAHMO

    def setUp(self):
        """Init test case."""
        shp_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'Kenya.geojson'
        )
        data_source = DataSource(shp_path)
        layer = data_source[0]
        for feature in layer:
            geometry = GEOSGeometry(feature.geom.wkt, srid=4326)
            Country.objects.create(
                name=feature['name'],
                iso_a3=feature['iso_a3'],
                geometry=geometry
            )
        self.dataset = Dataset.objects.get(
            name='Tahmo Ground Observational'
        )
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'TAHMO test stations.zip'
        )
        with open(filepath, 'rb') as _file:
            self.correct_file = SimpleUploadedFile(_file.name, _file.read())

    def _assert_correct_measurements(self):
        measurements = Station.objects.get(
            code='TA00001'
        ).measurement_set.all()
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_min'
                ).values_list('value', flat=True)
            ),
            [14.859377, 17.1, 17.2]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_max'
                ).values_list('value', flat=True)
            ),
            [24.111788, 30.9, 24]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_avg'
                ).values_list('value', flat=True)
            ),
            [19.1567865, 21.15, 18.47916667]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='rh_min'
                ).values_list('value', flat=True)
            ),
            [0.651, 0.429, 0.682]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='rh_max'
                ).values_list('value', flat=True)
            ),
            [0.995, 1, 1]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='ra_total'
                ).values_list('value', flat=True)
            ),
            [2070.156154, 5973, 1836]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='pr_total'
                ).values_list('value', flat=True)
            ),
            [1.885, 1.7, 15.521]
        )

    def test_no_file(self):
        """Test create provider object."""
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type,
            trigger_task=False
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_correct(self):
        """Test create provider object."""
        session = IngestorSession.objects.create(
            file=self.correct_file,
            ingestor_type=self.ingestor_type,
            trigger_task=False
        )
        session.run()
        session.refresh_from_db()
        self.assertEqual(
            session.ingestorsessionprogress_set.filter(
                session=session,
                status=IngestorSessionStatus.FAILED
            ).count(), 0
        )
        session.delete()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        for station in Station.objects.all():
            self.assertEqual(station.country.name, 'Kenya')
            self.assertEqual(station.country.iso_a3, 'KEN')
        self._assert_correct_measurements()

    def test_with_reset_data_config(self):
        """Test reset data before ingestor runs."""
        station = StationFactory.create(
            code='ABCDEF',
            provider=self.dataset.provider
        )
        ds_attr = DatasetAttribute.objects.get(
            dataset=self.dataset,
            attribute__variable_name='min_air_temperature'
        )
        measurement = MeasurementFactory.create(
            station=station,
            dataset_attribute=ds_attr
        )
        session = IngestorSession.objects.create(
            file=self.correct_file,
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False,
            additional_config={
                'reset_data': True
            }
        )
        session.run()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 3)
        self.assertFalse(
            Measurement.objects.filter(
                id=measurement.id
            ).exists()
        )
        self.assertEqual(
            Measurement.objects.filter(
                station=station
            ).count(),
            0
        )
        self._assert_correct_measurements()

    def test_with_min_date_config(self):
        """Test only ingest data after minimum date."""
        session = IngestorSession.objects.create(
            file=self.correct_file,
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False,
            additional_config={
                'min_date': '2018-03-14'
            }
        )
        session.run()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        measurements = Station.objects.get(
            code='TA00001'
        ).measurement_set.all()
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_min'
                ).values_list('value', flat=True)
            ),
            [17.1, 17.2]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_max'
                ).values_list('value', flat=True)
            ),
            [30.9, 24]
        )
