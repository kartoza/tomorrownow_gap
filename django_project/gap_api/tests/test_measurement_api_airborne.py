# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""
import csv
import io

from django.contrib.gis.geos import Point
from django.utils import dateparse

from gap.ingestor.wind_borne_systems import (
    PROVIDER, STATION_TYPE, DATASET_TYPE, DATASET_NAME
)
from gap.models import (
    Provider, StationType, Station, StationHistory, DatasetAttribute,
    Dataset, DatasetType, DatasetTimeStep, DatasetStore, Measurement
)
from gap_api.api_views.measurement import MeasurementAPI
from gap_api.tests.test_measurement_api import CommonMeasurementAPITest


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
    data = [
        {
            'geom': Point(
                x=0,
                y=0,
                srid=4326
            ),
            'time': '2000-01-01T00:00:00.000Z',
            'altitude': 1,
            'obs': {
                'Temperature': 1,
                'Atmospheric Pressure': 100
            }
        },
        {
            'geom': Point(
                x=10,
                y=10,
                srid=4326
            ),
            'time': '2000-02-01T00:00:00.000Z',
            'altitude': 2,
            'obs': {
                'Temperature': 2,
                'Atmospheric Pressure': 200
            }
        },
        {
            'geom': Point(
                x=100,
                y=100,
                srid=4326
            ),
            'time': '2000-03-01T00:00:00.000Z',
            'altitude': 3,
            'obs': {
                'Temperature': 3,
                'Atmospheric Pressure': 300
            }
        }
    ]

    def setUp(self):
        """Init test class."""
        super().setUp()

        # Prepare data
        provider = Provider.objects.get(
            name=PROVIDER
        )
        station_type = StationType.objects.get(
            name=STATION_TYPE
        )
        dataset_type = DatasetType.objects.get(
            name=DATASET_TYPE
        )
        dataset = Dataset.objects.get(
            name=DATASET_NAME,
            provider=provider,
            type=dataset_type,
            time_step=DatasetTimeStep.DAILY,
            store_type=DatasetStore.TABLE
        )
        station = Station.objects.create(
            provider=provider,
            station_type=station_type,
            code='test-1',
            name='test-1',
            geometry=Point(
                x=1,
                y=1,
                srid=4326
            ),
            altitude=1
        )
        for row in self.data:
            date_time = dateparse.parse_datetime(row['time'])
            history = StationHistory.objects.create(
                station=station,
                date_time=date_time,
                geometry=row['geom'],
                altitude=row['altitude']
            )
            for name, val in row['obs'].items():
                attribute = DatasetAttribute.objects.get(
                    dataset=dataset,
                    attribute__name=name,
                )
                Measurement.objects.update_or_create(
                    station=station,
                    dataset_attribute=attribute,
                    date_time=date_time,
                    defaults={
                        'value': val,
                        'station_history': history
                    }
                )

    def test_read_point(self):
        """Test read point."""
        view = MeasurementAPI.as_view()
        request = self._get_measurement_request_point(
            lat=0, lon=0,
            start_dt='2000-01-01', end_dt='2000-03-01',
            attributes=','.join(['atmospheric_pressure', 'temperature']),
            product='windborne_observational',
            output_type='json',
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        results = response.data['results']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['geometry']['coordinates'], [0.0, 0.0])
        self.assertEqual(results[0]['altitude'], 1)
        self.assertEqual(len(results[0]['data']), 1)
        self.assertEqual(
            results[0]['data'][0]['datetime'], '2000-01-01T00:00:00+00:00'
        )
        self.assertEqual(
            results[0]['data'][0]['values'],
            {'atmospheric_pressure': 100.0, 'temperature': 1.0}
        )

        # Getting lat lon 10,10
        request = self._get_measurement_request_point(
            lat=10, lon=10,
            start_dt='2000-01-01', end_dt='2000-03-01',
            attributes=','.join(['atmospheric_pressure', 'temperature']),
            product='windborne_observational',
            output_type='json',
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        results = response.data['results']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['geometry']['coordinates'], [10.0, 10.0])
        self.assertEqual(results[0]['altitude'], 2)
        self.assertEqual(len(results[0]['data']), 1)
        self.assertEqual(
            results[0]['data'][0]['datetime'], '2000-02-01T00:00:00+00:00'
        )
        self.assertEqual(
            results[0]['data'][0]['values'],
            {'atmospheric_pressure': 200.0, 'temperature': 2.0}
        )

    def read_csv(self, response):
        """Read csv file."""
        response_text = ''.join(
            chunk.decode('utf-8') for chunk in response.streaming_content
        )
        csv_file = io.StringIO(response_text)
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader, None)
        ordered_headers = [
            'date', 'lat', 'lon', 'altitude', 'atmospheric_pressure',
            'temperature'
        ]
        rows = []
        for row in csv_reader:
            row_data = list(range(len(headers)))
            for idx, header in enumerate(headers):
                row_data[ordered_headers.index(header)] = row[idx]
            rows.append(row_data)
        return ordered_headers, rows

    def test_read_with_bbox(self):
        """Test read point."""
        view = MeasurementAPI.as_view()
        request = self._get_measurement_request_bbox(
            bbox='0,0,100,100',
            start_dt='2000-02-01', end_dt='2000-03-01',
            attributes=','.join(['atmospheric_pressure', 'temperature']),
            product='windborne_observational',
            output_type='csv',
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['content-type'], 'text/csv')
        headers, rows = self.read_csv(response)
        self.assertEqual(
            headers,
            ['date', 'lat', 'lon', 'altitude', 'atmospheric_pressure',
             'temperature']
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            rows[0], ['2000-02-01', '10.0', '10.0', '2.0', '200.0', '2.0']
        )
        self.assertEqual(
            rows[1], ['2000-03-01', '100.0', '100.0', '3.0', '300.0', '3.0']
        )

        # Second request
        request = self._get_measurement_request_bbox(
            bbox='5,5,20,20',
            start_dt='2000-01-01', end_dt='2000-03-01',
            attributes=','.join(['atmospheric_pressure', 'temperature']),
            product='windborne_observational',
            output_type='csv',
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['content-type'], 'text/csv')
        headers, rows = self.read_csv(response)
        self.assertEqual(
            headers,
            ['date', 'lat', 'lon', 'altitude', 'atmospheric_pressure',
             'temperature']
        )
        self.assertEqual(
            headers,
            ['date', 'lat', 'lon', 'altitude', 'atmospheric_pressure',
             'temperature']
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0], ['2000-02-01', '10.0', '10.0', '2.0', '200.0', '2.0']
        )

        # Return data with altitude between 1.5-5, should return history 2 & 3
        request = self._get_measurement_request_bbox(
            bbox='0,0,100,100',
            altitudes='1.5,5',
            start_dt='2000-01-01', end_dt='2000-03-01',
            attributes=','.join(['atmospheric_pressure', 'temperature']),
            product='windborne_observational',
            output_type='csv',
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['content-type'], 'text/csv')
        headers, rows = self.read_csv(response)
        self.assertEqual(
            headers,
            ['date', 'lat', 'lon', 'altitude', 'atmospheric_pressure',
             'temperature']
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            rows[0], ['2000-02-01', '10.0', '10.0', '2.0', '200.0', '2.0']
        )
        self.assertEqual(
            rows[1], ['2000-03-01', '100.0', '100.0', '3.0', '300.0', '3.0']
        )
