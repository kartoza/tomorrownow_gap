# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

import json
from datetime import datetime
from typing import List
from unittest.mock import patch

from django.contrib.gis.geos import Polygon, MultiPolygon, Point
from django.urls import reverse

from core.tests.common import FakeResolverMatchV1, BaseAPIViewTest
from gap.factories import (
    StationFactory,
    MeasurementFactory
)
from gap.models import DatasetAttribute, Dataset
from gap.utils.reader import (
    DatasetReaderValue, DatasetTimelineValue,
    DatasetReaderInput, DatasetReaderOutputType, BaseDatasetReader,
    LocationInputType
)
from gap_api.api_views.measurement import MeasurementAPI


class MockDatasetReader(BaseDatasetReader):
    """Class to mock a dataset reader."""

    def __init__(
            self, dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            output_type=DatasetReaderOutputType.JSON,
            altitudes: (float, float) = None
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
            [DatasetTimelineValue(self.start_date, {
                'test': 100
            }, p)],
            DatasetReaderInput.from_point(p),
            self.attributes
        )


class CommonMeasurementAPITest(BaseAPIViewTest):
    """Common class for Measurement API Test."""

    def _get_measurement_request(
            self, lat=None, lon=None, bbox=None,
            attributes='max_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', product=None,
            output_type='json', altitudes=None
    ):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param bbox: Bounding box: xmin, ymin, xmax, ymax
        :type bbox: str, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :param product: product type, defaults to None
        :type product: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        request_params = (
            f'?attributes={attributes}'
            f'&start_date={start_dt}&end_date={end_dt}'
            f'&output_type={output_type}'
        )
        if product:
            request_params = request_params + f'&product={product}'
        if altitudes:
            request_params = request_params + f'&altitudes={altitudes}'
        if bbox:
            request_params = request_params + f'&bbox={bbox}'
        if lat is not None and lon is not None:
            request_params = request_params + f'&lat={lat}&lon={lon}'
        request = self.factory.get(
            reverse('api:v1:get-measurement') + request_params
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        return request

    def _get_measurement_request_point(
            self, lat=-2.215, lon=29.125, attributes='max_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', product=None,
            output_type='json', altitudes=None
    ):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :param product: product type, defaults to None
        :type product: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        return self._get_measurement_request(
            lat=lat, lon=lon, attributes=attributes,
            start_dt=start_dt, end_dt=end_dt, product=product,
            output_type=output_type, altitudes=altitudes
        )

    def _get_measurement_request_bbox(
            self, bbox='0,0,10,10', attributes='max_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', product=None,
            output_type='json', altitudes=None
    ):
        """Get request for Measurement API.

        :param bbox: Bounding box: xmin, ymin, xmax, ymax,
            defaults to '0,0,10,10'
        :type bbox: str, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :param product: product type, defaults to None
        :type product: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        return self._get_measurement_request(
            bbox=bbox, attributes=attributes,
            start_dt=start_dt, end_dt=end_dt, product=product,
            output_type=output_type, altitudes=altitudes
        )

    def _post_measurement_request(
            self, lat=-2.215, lon=29.125, attributes='max_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', product=None,
            output_type='json'):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :param product: product type, defaults to None
        :type product: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        request_params = (
            f'?attributes={attributes}'
            f'&start_date={start_dt}&end_date={end_dt}'
            f'&output_type={output_type}'
        )
        if product:
            request_params = request_params + f'&product={product}'
        polygon = Polygon(((0, 0), (0, 10), (10, 10), (10, 0), (0, 0)))
        data = {
            "type": "FeatureCollection",
            "name": "polygon",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "1"},
                    "geometry": json.loads(MultiPolygon(polygon).json)
                }
            ]
        }
        request = self.factory.post(
            reverse('api:v1:get-measurement') + request_params,
            data=data, format='json'
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        return request


class HistoricalAPITest(CommonMeasurementAPITest):
    """Historical api test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def test_read_historical_data_empty(self):
        """Test read historical data that returns empty."""
        view = MeasurementAPI.as_view()
        request = self._get_measurement_request_point()
        response = view(request)
        self.assertEqual(response.status_code, 404)

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_read_historical_data(self, mocked_reader):
        """Test read historical data."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockDatasetReader
        dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribute2 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='total_rainfall'
        ).first()
        attribs = [
            attribute1.attribute.variable_name,
            attribute2.attribute.variable_name
        ]
        request = self._get_measurement_request_point(
            attributes=','.join(attribs)
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertIn('metadata', response.data)
        self.assertIn('results', response.data)
        results = response.data['results']
        self.assertEqual(len(results), 1)
        result_data = results[0]['data']
        self.assertIn('values', result_data[0])
        self.assertIn('test', result_data[0]['values'])
        self.assertEqual(100, result_data[0]['values']['test'])
        # with product type
        request = self._get_measurement_request_point(
            attributes=','.join(attribs),
            product='test_empty'
        )
        response = view(request)
        self.assertEqual(response.status_code, 400)
        self.assertIn('Invalid Request Parameter', response.data)

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_read_historical_data_by_polygon(self, mocked_reader):
        """Test read historical data."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockDatasetReader
        dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribute2 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='total_rainfall'
        ).first()
        attribs = [
            attribute1.attribute.variable_name,
            attribute2.attribute.variable_name
        ]
        request = self._post_measurement_request(
            attributes=','.join(attribs),
            output_type='json'
        )
        response = view(request)
        self.assertEqual(response.status_code, 400)
        self.assertIn('Invalid Request Parameter', response.data)
        request = self._post_measurement_request(
            attributes=','.join(attribs),
            output_type='csv'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertEqual(response['content-type'], 'text/csv')
        mocked_reader.reset_mock()
        request = self._post_measurement_request(
            attributes=','.join(attribs),
            output_type='ascii'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertEqual(response['content-type'], 'text/ascii')
        mocked_reader.reset_mock()
        request = self._post_measurement_request(
            attributes=','.join(attribs),
            output_type='netcdf'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertEqual(response['content-type'], 'application/x-netcdf')
        # invalid output_type
        request = self._post_measurement_request(
            attributes=','.join(attribs),
            output_type='sdfsdf'
        )
        response = view(request)
        self.assertEqual(response.status_code, 400)
        self.assertIn('Invalid Request Parameter', response.data)

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_validate_dataset_attributes(self, mocked_reader):
        """Test validate dataset attributes ensembles."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockDatasetReader
        dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='temperature_anom'
        ).first()
        attribute2 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='temperature_clim'
        ).first()
        attribs = [
            attribute1.attribute.variable_name,
            attribute2.attribute.variable_name
        ]
        request = self._get_measurement_request_point(
            attributes=','.join(attribs),
            product='seasonal_forecast',
            output_type='csv'
        )
        response = view(request)
        self.assertEqual(response.status_code, 400)
        self.assertIn('ensemble', response.data['Invalid Request Parameter'])

    def test_read_from_tahmo(self):
        """Test validate dataset attributes ensembles."""
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
            attributes=','.join(attribs),
            product='tahmo_ground_observation',
            output_type='csv'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['content-type'], 'text/csv')
