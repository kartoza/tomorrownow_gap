# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for UserFile.
"""

from datetime import timedelta, datetime
from typing import List, Tuple
from unittest.mock import patch
from django.utils import timezone
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.contrib.gis.geos import Point

from core.utils.s3 import create_s3_bucket, remove_s3_folder
from gap.models import DatasetAttribute, Dataset, Preferences
from gap_api.models import UserFile
from gap_api.tasks.cleanup import cleanup_user_files
from gap.utils.reader import (
    DatasetReaderValue,
    DatasetReaderInput, DatasetReaderOutputType, BaseDatasetReader,
    LocationInputType
)
from gap_api.api_views.measurement import MeasurementAPI

from gap.factories import MeasurementFactory, StationFactory
from gap.tests.ingestor.test_cbam_bias_adjust import mock_open_zarr_dataset
from gap_api.tests.test_measurement_api import CommonMeasurementAPITest
from gap_api.factories import UserFileFactory


class MockXArrayDatasetReader(BaseDatasetReader):
    """Class to mock a dataset reader."""

    def __init__(
            self, dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            output_type=DatasetReaderOutputType.JSON,
            altitudes: Tuple[float, float] = None
    ) -> None:
        """Initialize MockDatasetReader class."""
        super().__init__(
            dataset, attributes, location_input,
            start_date, end_date, output_type)

    def get_data_values(self) -> DatasetReaderValue:
        """Override data values with a mock object."""
        if self.location_input.type == LocationInputType.POLYGON:
            p = Point(0, 0)
        else:
            p = self.location_input.point
        return DatasetReaderValue(
            mock_open_zarr_dataset(),
            DatasetReaderInput.from_point(p),
            self.attributes
        )


class TestUserFileAPI(CommonMeasurementAPITest):
    """Test UserFile in the API."""

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
        """Set the test class."""
        super().setUp()
        preferences = Preferences.load()
        preferences.api_use_x_accel_redirect = True
        preferences.save()

        self.s3_storage: S3Boto3Storage = storages["gap_products"]
        create_s3_bucket(self.s3_storage.bucket.name)

    def tearDown(self):
        """Cleanup resources."""
        remove_s3_folder(self.s3_storage, 'dev/user_data')
        super().tearDown()

    def test_cleanup(self):
        """Test the cleanup logic."""
        f1 = UserFileFactory.create()
        self.s3_storage.save(f1.name, ContentFile('echo'))

        f2 = UserFileFactory.create()
        f2.name = 'dev/user_data/124.csv'
        f2.created_on = timezone.now() - timedelta(days=15)
        f2.save()
        self.s3_storage.save(f2.name, ContentFile('echo'))

        cleanup_user_files()

        self.assertTrue(
            UserFile.objects.filter(
                id=f1.id
            ).exists()
        )
        self.assertFalse(
            UserFile.objects.filter(
                id=f2.id
            ).exists()
        )
        self.assertTrue(self.s3_storage.exists(f1.name))
        self.assertFalse(self.s3_storage.exists(f2.name))

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_api_netcdf_request(self, mocked_reader):
        """Test generate to netcdf."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockXArrayDatasetReader
        dataset = Dataset.objects.get(
            type__variable_name='cbam_historical_analysis_bias_adjust'
        )
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribs = [attribute1.attribute.variable_name]
        point = Point(x=26.9665, y=-12.5969)
        request = self._get_measurement_request_point(
            product='cbam_historical_analysis_bias_adjust',
            attributes=','.join(attribs),
            lat=point.y, lon=point.x,
            start_dt='2023-01-01',
            end_dt='2023-01-01',
            output_type='netcdf'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertIn('X-Accel-Redirect', response.headers)
        self.assertTrue(UserFile.objects.filter(
            user=self.superuser,
            query_params__output_type='netcdf',
            query_params__product='cbam_historical_analysis_bias_adjust',
            query_params__geom_type='point',
            query_params__geometry=point.wkt,
            query_params__start_date='2023-01-01',
            query_params__end_date='2023-01-01'
        ).exists())

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_api_csv_request(self, mocked_reader):
        """Test generate to csv."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockXArrayDatasetReader
        dataset = Dataset.objects.get(
            type__variable_name='cbam_historical_analysis_bias_adjust'
        )
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribs = [attribute1.attribute.variable_name]
        point = Point(x=26.9665, y=-12.5969)
        request = self._get_measurement_request_point(
            product='cbam_historical_analysis_bias_adjust',
            attributes=','.join(attribs),
            lat=point.y, lon=point.x,
            start_dt='2023-01-01',
            end_dt='2023-01-01',
            output_type='csv'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertIn('X-Accel-Redirect', response.headers)
        self.assertTrue(UserFile.objects.filter(
            user=self.superuser,
            query_params__output_type='csv',
            query_params__product='cbam_historical_analysis_bias_adjust',
            query_params__geom_type='point',
            query_params__geometry=point.wkt,
            query_params__start_date='2023-01-01',
            query_params__end_date='2023-01-01'
        ).exists())

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_api_cached_request(self, mocked_reader):
        """Test cached UserFile."""
        f2 = UserFileFactory.create()

        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockXArrayDatasetReader
        dataset = Dataset.objects.get(
            type__variable_name='cbam_historical_analysis_bias_adjust'
        )
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribs = [attribute1.attribute.variable_name]
        request = self._get_measurement_request_point(
            product='cbam_historical_analysis_bias_adjust',
            attributes=','.join(attribs),
            lat=1, lon=1,
            start_dt='2020-01-01',
            end_dt='2020-01-02',
            output_type='csv'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertIn('X-Accel-Redirect', response.headers)
        self.assertIn(f2.name, response.headers['X-Accel-Redirect'])

    def test_api_observation_csv_request(self):
        """Test Observation API to csv."""
        view = MeasurementAPI.as_view()
        dataset = Dataset.objects.get(name='Tahmo Ground Observational')
        p = Point(x=26.97, y=-12.56, srid=4326)
        station = StationFactory.create(
            geometry=p,
            provider=dataset.provider
        )
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='min_relative_humidity'
        ).first()
        dt = datetime(2019, 11, 1, 0, 0, 0)
        MeasurementFactory.create(
            station=station,
            dataset_attribute=attribute1,
            date_time=dt,
            value=100
        )
        attribs = [
            attribute1.attribute.variable_name
        ]
        request = self._get_measurement_request_point(
            lat=p.y,
            lon=p.x,
            attributes=','.join(attribs),
            product='tahmo_ground_observation',
            output_type='csv',
            start_dt=dt.date().isoformat(),
            end_dt=dt.date().isoformat()
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn('X-Accel-Redirect', response.headers)
        self.assertTrue(UserFile.objects.filter(
            user=self.superuser,
            query_params__output_type='csv',
            query_params__product='tahmo_ground_observation',
            query_params__geom_type='point',
            query_params__geometry=p.wkt
        ).exists())

    def test_api_observation_netcdf_request(self):
        """Test cached UserFile."""
        view = MeasurementAPI.as_view()
        dataset = Dataset.objects.get(name='Tahmo Ground Observational')
        p = Point(x=26.97, y=-12.56, srid=4326)
        station = StationFactory.create(
            geometry=p,
            provider=dataset.provider
        )
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='min_relative_humidity'
        ).first()
        dt = datetime(2019, 11, 1, 0, 0, 0)
        MeasurementFactory.create(
            station=station,
            dataset_attribute=attribute1,
            date_time=dt,
            value=100
        )
        attribs = [
            attribute1.attribute.variable_name
        ]
        request = self._get_measurement_request_point(
            lat=p.y,
            lon=p.x,
            attributes=','.join(attribs),
            product='tahmo_ground_observation',
            output_type='netcdf',
            start_dt=dt.date().isoformat(),
            end_dt=dt.date().isoformat()
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn('X-Accel-Redirect', response.headers)
        self.assertTrue(UserFile.objects.filter(
            user=self.superuser,
            query_params__output_type='netcdf',
            query_params__product='tahmo_ground_observation',
            query_params__geom_type='point',
            query_params__geometry=p.wkt
        ).exists())
