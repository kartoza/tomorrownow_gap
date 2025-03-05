# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import os
import logging
from django.core.management.base import BaseCommand
from django.contrib.gis.geos import Point
import datetime
import pytz

from gap.models import Dataset, DataSourceFile, DatasetStore, DatasetAttribute
from gap.providers import TioZarrReader, TomorrowIODatasetReader
from gap.utils.reader import DatasetReaderInput


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to read tio zarr."""

    def read_api(self):
        dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        attributes = DatasetAttribute.objects.filter(
            source__in=['temperatureMax', 'temperatureMin', 'humidityMax', 'humidityMin']
        )
        # p = Point(x=35.73590167, y=0.58588279)
        p = Point(x=35.736, y=0.586)
        start_dt = datetime.datetime(
            2025, 3, 5, 0, 0, 0, tzinfo=pytz.UTC
        )
        end_dt = datetime.datetime(
            2025, 3, 9, 0, 0, 0, tzinfo=pytz.UTC
        )
        reader = TomorrowIODatasetReader(
            dataset,
            attributes,
            DatasetReaderInput.from_point(p),
            start_dt,
            end_dt
        )
        reader.read()
        values = reader.get_data_values()
        print(values.to_json())

    def handle(self, *args, **options):
        """Run read tio zarr."""

        # self.read_api()

        dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        data_source = DataSourceFile.objects.filter(
            dataset=dataset,
            is_latest=True
        ).last()
        reader = TioZarrReader(
            dataset,
            [],
            None,
            None,
            None
        )
        reader.setup_reader()
        ds = reader.open_dataset(data_source)

        lat = 0.58588279
        lon = 35.73590167

        date = '2025-03-04'
        attributes = ['max_temperature', 'min_temperature', 'humidity_maximum', 'humidity_minimum']

        ds = ds[attributes].sel(
            forecast_date=date,
            forecast_day_idx=slice(0, 4)
        ).sel(
            lat=lat,
            lon=lon, method='nearest'
        )
        
        print('max_temperature')
        print(ds['max_temperature'].values)
        print('min_temperature')
        print(ds['min_temperature'].values)
        print('humidity_max')
        print(ds['humidity_maximum'].values)
        print('humidity_min')
        print(ds['humidity_minimum'].values)
