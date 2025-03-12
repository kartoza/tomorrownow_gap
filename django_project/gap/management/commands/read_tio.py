# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import os
import json
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
            source__in=[
                'temperatureMax',
                'temperatureMin',
                'humidityMax',
                'humidityMin',
                'rainAccumulationSum',
                'evapotranspirationSum',
                'solarGHISum',
                'weatherCode',
                'floodIndex'
            ]
        )
        # p = Point(x=35.73590167, y=0.58588279)
        # p = Point(x=35.736, y=0.586)
        names = [
            "SE03769900275",
            "SE03740800526",
            "NE03500900191",
            "SE03460900598",
            "NE03431800407",
            "NE03559000550",
            "SE03726300705",
            "SE03537200813",
            "SE03675401960",
            "SE03704502283",
            "SE03802601422",
            "SE03719001566",
            "SE03762601351",
            "SE03773502211",
            "NE03780800084",
            "SE03460900633"
        ]

        points = [
            Point(y=-0.27483257, x=37.69897987),
            Point(y=-0.52587455, x=37.40815347),
            Point(y=0.19138825, x=35.00883567),
            Point(y=-0.59760083, x=34.60894937),
            Point(y=0.40656709, x=34.31812297),
            Point(y=0.55001965, x=35.59048847),
            Point(y=-0.70519025, x=37.26274027),
            Point(y=-0.81277967, x=35.37236867),
            Point(y=-1.96040015, x=36.75379407),
            Point(y=-2.28316841, x=37.04462047),
            Point(y=-1.42245305, x=38.02615957),
            Point(y=-1.56590561, x=37.19003367),
            Point(y=-1.35072677, x=37.62627327),
            Point(y=-2.21144213, x=37.73533317),
            Point(y=0.08379, x=37.8083),
            Point(y=-0.6334, x=34.6089),
        ]
        for idx, p in enumerate(points):
            start_dt = datetime.datetime(
                2025, 3, 6, 0, 0, 0, tzinfo=pytz.UTC
            )
            end_dt = datetime.datetime(
                2025, 3, 27, 0, 0, 0, tzinfo=pytz.UTC
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
            # print(values.to_json())
            with open(f'/home/web/project/django_project/{names[idx]}_lat{p.y}_lon{p.x}.json', 'w') as f:
                json.dump(values.to_json(), f, indent=4)

    def handle(self, *args, **options):
        """Run read tio zarr."""

        self.read_api()

        # dataset = Dataset.objects.get(
        #     name='Tomorrow.io Short-term Forecast',
        #     store_type=DatasetStore.ZARR
        # )
        # data_source = DataSourceFile.objects.filter(
        #     dataset=dataset,
        #     is_latest=True
        # ).last()
        # reader = TioZarrReader(
        #     dataset,
        #     [],
        #     None,
        #     None,
        #     None
        # )
        # reader.setup_reader()
        # ds = reader.open_dataset(data_source)
        # # print(ds)

        # # lat = 0.58588279
        # # lon = 35.73590167

        # lat = 0.15552511
        # lon = 35.91766817

        # date = '2025-03-04'
        # attributes = [
        #     'max_temperature',
        #     'min_temperature',
        #     'humidity_maximum',
        #     'humidity_minimum',
        #     'total_rainfall',
        #     'total_evapotranspiration_flux'
        # ]

        # ds = ds[attributes].sel(
        #     forecast_date=date,
        #     forecast_day_idx=slice(0, 3)
        # ).sel(
        #     lat=lat,
        #     lon=lon, method='nearest'
        # )
        
        # print('max_temperature')
        # print(ds['max_temperature'].values)
        # print('min_temperature')
        # print(ds['min_temperature'].values)
        # print('humidity_max')
        # print(ds['humidity_maximum'].values)
        # print('humidity_min')
        # print(ds['humidity_minimum'].values)
        # print('total_rainfall')
        # print(ds['total_rainfall'].values)
        # print('total_evapotranspiration_flux')
        # print(ds['total_evapotranspiration_flux'].values)
