# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Location API.
"""

import datetime
import pytz
from mock import patch
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile

from core.settings.utils import absolute_path
from core.tests.common import FakeResolverMatchV1, BaseAPIViewTest
from gap.utils.reader import LocationInputType
from gap_api.models.location import Location
from gap_api.api_views.location import LocationAPI
from gap_api.factories import LocationFactory
from gap_api.tasks.cleanup import cleanup_user_locations


class TestAPILocation(BaseAPIViewTest):
    """Test class for location API and task."""

    def _get_request(self, file_path, name=None, file_name=None):
        """Get request for test upload."""
        request_params = ''
        if name:
            request_params = f'?location_name={name}'
        with open(file_path, 'rb') as data:
            file = SimpleUploadedFile(
                content=data.read(),
                name=file_name if file_name else data.name,
                content_type='multipart/form-data'
            )
        request = self.factory.post(
            reverse('api:v1:upload-location') + request_params,
            data={
                'file': file
            }
        )
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        return request

    def _check_error(
            self, response, error_detail, status_code = 400,
            error_key='Invalid Request Parameter'):
        """Check for error in the response."""
        self.assertEqual(response.status_code, status_code)
        self.assertIn(error_key, response.data)
        self.assertEqual(str(response.data[error_key]), error_detail)

    def _check_success(self, response, check_type = None) -> Location:
        """Check success response."""
        self.assertEqual(response.status_code, 200)
        self.assertIn('location_name', response.data)
        self.assertIn('expired_on', response.data)
        location = Location.objects.filter(
            user=self.user_1,
            name=response.data['location_name']
        ).first()
        self.assertTrue(location)
        if check_type:
            print(f'geomtypeid {location.geometry.geom_typeid}')
            self.assertEqual(
                LocationInputType.map_from_geom_typeid(
                    location.geometry.geom_typeid),
                check_type
            )
        return location

    @patch('django.utils.timezone.now')
    def test_cleanup_user_locations(self, mock_timezone):
        """Test cleanup expired locations."""
        mock_timezone.return_value = datetime.datetime(
            2024, 10, 1, 0, 0, 0, tzinfo=pytz.utc
        )
        # expired_on will be assigned created_on+60days
        location1 = LocationFactory.create(
            user=self.user_1,
            created_on=datetime.datetime(
                2024, 7, 30, 0, 0, 0, tzinfo=pytz.utc
            )
        )
        location2 = LocationFactory.create(
            user=self.user_1,
            created_on=datetime.datetime(
                2024, 9, 1, 0, 0, 0, tzinfo=pytz.utc
            )
        )
        cleanup_user_locations()
        self.assertFalse(Location.objects.filter(
            id=location1.pk
        ).exists())
        self.assertTrue(Location.objects.filter(
            id=location2.pk
        ).exists())

    def test_upload_empty_name(self):
        """Test upload with empty name."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'point.zip')
        request = self._get_request(file_path)
        response = view(request)
        self._check_error(response, 'location_name is mandatory!', 400)

    def test_upload_invalid_type(self):
        """Test upload with invalid file type."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'point.zip')
        request = self._get_request(file_path, 'test1', 'test.txt')
        response = view(request)
        self._check_error(response, 'Unrecognized file type!', 400)

    def test_upload_invalid_shapefile(self):
        """Test upload with invalid shapefile."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'shp_no_shp.zip')
        request = self._get_request(file_path, 'test1')
        response = view(request)
        self._check_error(
            response,
            'Missing required file(s) inside zip file: \n- shp_1_1.shp',
            400
        )

    def test_upload_invalid_crs(self):
        """Test upload with invalid crs."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'shp_3857.zip')
        request = self._get_request(file_path, 'test1')
        response = view(request)
        self._check_error(
            response,
            'Incorrect CRS type: epsg:3857!',
            400
        )

    def test_upload_invalid_geom_type(self):
        """Test upload with invalid geom type."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'line.zip')
        request = self._get_request(file_path, 'test1')
        response = view(request)
        self._check_error(
            response,
            'Geometry type LineString is not allowed!',
            400
        )

    def test_upload_point(self):
        """Test upload with point."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'point.zip')
        request = self._get_request(file_path, 'test1')
        response = view(request)
        self._check_success(response, check_type=LocationInputType.POINT)

    def test_upload_polygons(self):
        """Test upload with polygons."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'polygons.zip')
        request = self._get_request(file_path, 'polygons')
        response = view(request)
        self._check_success(response, check_type=LocationInputType.POLYGON)

    def test_upload_multipolygon(self):
        """Test upload with multipolygon."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'multipolygon.zip')
        request = self._get_request(file_path, 'multipolygon')
        response = view(request)
        self._check_success(response, check_type=LocationInputType.POLYGON)

    def test_upload_multipoint(self):
        """Test upload with multipoint."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'multipoint.geojson')
        request = self._get_request(file_path, 'test_multipoint')
        response = view(request)
        self._check_success(
            response, check_type=LocationInputType.LIST_OF_POINT)

    def test_upload_gpkg(self):
        """Test upload with geopackage."""
        view = LocationAPI.as_view()
        file_path = absolute_path(
            'gap_api', 'tests', 'utils', 'data', 'gpkg.gpkg')
        request = self._get_request(file_path, 'test_gpkg')
        response = view(request)
        self._check_success(
            response, check_type=LocationInputType.POLYGON)
