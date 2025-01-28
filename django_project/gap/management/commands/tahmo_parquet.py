# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Command to test writing Tahmo parquet files.
"""

import os
import pandas as pd
import geopandas as gpd
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
from django.core.management.base import BaseCommand
from django.db.models import F
from django.db.models.functions.datetime import TruncDate, ExtractYear
from shapely.wkt import loads
from django.contrib.gis.db.models.functions import AsWKT

from gap.models import (
    Measurement, Dataset, Country
)
from gap.providers.observation import ST_X, ST_Y



class Command(BaseCommand):
    """Command to export Tahmo Dataset to geoparquet."""

    def _get_s3_variables(self) -> dict:
        """Get s3 env variables for product bucket.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        prefix = 'MINIO'
        keys = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'AWS_ENDPOINT_URL', 'AWS_REGION_NAME'
        ]
        results = {}
        for key in keys:
            results[key] = os.environ.get(f'{prefix}_{key}', '')
        results['AWS_BUCKET_NAME'] = os.environ.get(
            'MINIO_GAP_AWS_BUCKET_NAME', '')
        results['AWS_DIR_PREFIX'] = os.environ.get(
            'MINIO_GAP_AWS_DIR_PREFIX', '')

        return results

    def get_s3_client_kwargs(cls) -> dict:
        """Get s3 client kwargs for Zarr file.

        :return: dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        prefix = 'MINIO'
        client_kwargs = {}
        if os.environ.get(f'{prefix}_AWS_ENDPOINT_URL', ''):
            client_kwargs['endpoint_url'] = os.environ.get(
                f'{prefix}_AWS_ENDPOINT_URL', '')
        if os.environ.get(f'{prefix}_AWS_REGION_NAME', ''):
            client_kwargs['region_name'] = os.environ.get(
                f'{prefix}_AWS_REGION_NAME', '')
        return client_kwargs

    def setup_reader(self):
        """Initialize s3fs."""
        self.s3 = self._get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self.get_s3_client_kwargs()
        }

    def _get_directory_path(self):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/tahmo/"
        )

    def handle(self, *args, **options):
        """Run the export tahmo dataset."""
        country = Country.objects.get(name='Kenya')
        dataset = Dataset.objects.get(
            name="Tahmo Ground Observational"
        )

        fields = {
            'date': (
                TruncDate('date_time')
            ),
            'loc_x': ST_X('geom'),
            'loc_y': ST_Y('geom'),
            'attr': F('dataset_attribute__attribute__variable_name'),
            'st_code': F('station__code'),
            'st_id': F('station__id'),
            'country': F('station__country'),
            'geometry': AsWKT('geom')
        }

        self.setup_reader()
        fs = fsspec.filesystem(
            's3',
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=self.get_s3_client_kwargs()
        )

        years = list(Measurement.objects.annotate(
            year=ExtractYear('date_time')
        ).filter(
            dataset_attribute__dataset=dataset,
            station__country=country
        ).order_by('year').distinct('year').values_list(
            'year',
            flat=True
        ))
        s3_path = self._get_directory_path()

        for year in years:
            measurements = Measurement.objects.filter(
                dataset_attribute__dataset=dataset,
                station__country=country,
                date_time__year=year
            ).order_by('date_time', 'station_id')
            print(f'Year {year} total_count: {measurements.count()}')

            measurements = measurements.annotate(
                geom=F('station__geometry'),
            ).annotate(**fields).values(
                *(list(fields.keys()) + ['value'])
            )

            # Convert to DataFrame
            df = pd.DataFrame(list(measurements))

            # Pivot the data to make attributes columns
            df = df.pivot_table(
                index=[
                    'date',
                    'loc_y',
                    'loc_x',
                    'st_code',
                    'st_id',
                    'geometry',
                    'country'
                ],
                columns='attr',
                values='value'
            ).reset_index()

            # Extract year, month, and day from the date column
            df['year'] = df['date'].apply(lambda x: x.year)
            df['month'] = df['date'].apply(lambda x: x.month)
            df['day'] = df['date'].apply(lambda x: x.day)

            # Create a GeoDataFrame
            df['geometry'] = df['geometry'].apply(
                lambda x: loads(x)
            )
            gdf = gpd.GeoDataFrame(df, geometry='geometry')

            # Set the CRS (update 'EPSG:4326'
            # if your geometry usesa different CRS)
            gdf.set_crs(epsg=4326, inplace=True)

            # View the GeoDataFrame
            # print(gdf)

            arrow_table = gdf.to_arrow()
            table = pa.table(arrow_table)

            pq.write_to_dataset(
                table,
                root_path=s3_path,
                partition_cols=['country', 'year', 'month'],
                filesystem=fs
            )
