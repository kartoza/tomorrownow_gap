# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading and writing Parquet Files
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from django.db.models import F
from django.db.models.functions.datetime import (
    ExtractYear,
    ExtractMonth,
    ExtractDay,
    TruncMonth
)
from django.contrib.gis.db.models import Union
from django.contrib.gis.db.models.functions import AsWKB
import duckdb
from django.conf import settings
from django.core.files.storage import storages

from core.models import ObjectStorageManager
from gap.models import (
    Measurement, Dataset, DataSourceFile, Station,
    DatasetAttribute, StationHistory
)
from gap.utils.geometry import ST_X, ST_Y
from gap.utils.ingestor_config import get_ingestor_config_from_preferences
from core.utils.date import split_epochs_by_year, split_epochs_by_year_month


logger = logging.getLogger(__name__)


class ParquetConverter:
    """Class to convert Measurement data to GeoParquet."""

    STATION_JOIN_KEY = 'st_id'
    WEATHER_FIELDS = {
        'dt': F('date_time'),
        'attr': F('dataset_attribute__attribute__variable_name'),
        'st_id': F('station__id'),
        'year': (
            ExtractYear('date_time')
        ),
        'month': (
            ExtractMonth('date_time')
        ),
        'day': (
            ExtractDay('date_time')
        ),
    }

    def __init__(
        self, dataset: Dataset, data_source: DataSourceFile, mode='w'
    ):
        """Initialize ParquetConverter class."""
        self.dataset = dataset
        self.mode = mode
        self.config = get_ingestor_config_from_preferences(dataset.provider)
        self.data_source = data_source
        self.attributes = [
            a.attribute.variable_name for a in
            DatasetAttribute.objects.select_related(
                'attribute'
            ).filter(
                dataset=dataset
            )
        ]

    def _get_s3_variables(self) -> dict:
        """Get s3 env variables for product bucket.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        results = ObjectStorageManager.get_s3_env_vars()
        return results

    def _get_s3_client_kwargs(cls, s3=None) -> dict:
        """Get s3 client kwargs for savign parquet file.

        :return: dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        if s3 is None:
            s3 = cls._get_s3_variables()

        client_kwargs = {}
        if s3.get('S3_ENDPOINT_URL'):
            client_kwargs['endpoint_url'] = s3['S3_ENDPOINT_URL']
        if s3.get('S3_REGION_NAME'):
            client_kwargs['region_name'] = s3['S3_REGION_NAME']
        return client_kwargs

    def setup(self):
        """Initialize s3fs."""
        self.s3 = self._get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('S3_ACCESS_KEY_ID'),
            'secret': self.s3.get('S3_SECRET_ACCESS_KEY'),
            'client_kwargs': self._get_s3_client_kwargs(
                self.s3
            )
        }

    def _get_directory_path(self, data_source: DataSourceFile):
        return (
            f"s3://{self.s3['S3_BUCKET_NAME']}/"
            f"{self.s3['S3_DIR_PREFIX']}/{data_source.name}/"
        )

    def _get_connection(self, s3):
        endpoint = s3['S3_ENDPOINT_URL']
        if settings.DEBUG:
            endpoint = endpoint.replace('http://', '')
        else:
            endpoint = endpoint.replace('https://', '')
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]

        config = {
            's3_access_key_id': s3['S3_ACCESS_KEY_ID'],
            's3_secret_access_key': s3['S3_SECRET_ACCESS_KEY'],
            's3_region': 'us-east-1',
            's3_url_style': 'path',
            's3_endpoint': endpoint,
            's3_use_ssl': not settings.DEBUG,
            'threads': 1
        }

        conn = duckdb.connect(config=config)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        return conn

    def _check_parquet_exists(self, s3_path: str, year: int, month=None):
        s3_storage = storages['gap_products']
        path = (
            f'{s3_path.replace(f's3://{self.s3['S3_BUCKET_NAME']}/', '')}'
            f'year={year}'
        )
        if month:
            path += f'/month={month}'
        _, files = s3_storage.listdir(path)
        return len(files) > 0

    def _store_dataframe_as_geoparquet(
        self, df: pd.DataFrame, s3_path, bbox, use_month=False
    ):
        print('Writing a new parquet file')
        conn = self._get_connection(self.s3)
        # copy df to duckdb table
        columns = [c for c in list(df.columns) if c != 'geom']
        sql = (
            f"""
            CREATE TABLE weather AS
            SELECT {','.join(columns)}, ST_GeomFromWKB(geom) AS geometry
            FROM df
            ORDER BY
            ST_Hilbert(
                ST_GeomFromWKB(geom),
                ST_Extent(ST_MakeEnvelope(
                {bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}))
            );
            """
        )
        conn.sql(sql)
        # export to parquet file
        partition_by = 'year'
        if use_month:
            partition_by += ',month'
        sql = (
            f"""
            COPY (SELECT * FROM weather)
            TO '{s3_path}'
            (FORMAT 'parquet', COMPRESSION 'zstd',
            PARTITION_BY ({partition_by}),
            OVERWRITE_OR_IGNORE true);
            """
        )
        conn.sql(sql)
        conn.close()

    def _append_dataframe_to_geoparquet(
        self, df: pd.DataFrame, s3_path, bbox, year, month=None
    ):
        print(f'Appending dataframe to existing {year}')
        conn = self._get_connection(self.s3)

        # copy original parquet to duckdb table
        parquet_dir = f'{s3_path}year=*/*.parquet'
        month_cond = ''
        if month:
            parquet_dir = f'{s3_path}year=*/month=*/*.parquet'
            month_cond = f'AND month={month}'
        sql = (
            f"""
            CREATE TABLE tmp_weather AS
            SELECT *
            FROM read_parquet('{parquet_dir}',
            hive_partitioning=true)
            WHERE year={year} {month_cond}
            """
        )
        conn.sql(sql)

        # insert df to duckdb table
        columns = [c for c in list(df.columns) if c != 'geom']
        sql = (
            f"""
            INSERT INTO tmp_weather BY NAME
            SELECT {','.join(columns)}, ST_GeomFromWKB(geom) AS geometry
            FROM df
            """
        )
        conn.sql(sql)

        # order the table
        sql = (
            f"""
            CREATE TABLE weather AS
            SELECT * FROM tmp_weather
            ORDER BY
            ST_Hilbert(
                geometry,
                ST_Extent(ST_MakeEnvelope(
                {bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}))
            );
            """
        )
        conn.sql(sql)

        # export to parquet file
        partition_by = 'year'
        if month:
            partition_by += ',month'
        sql = (
            f"""
            COPY (SELECT * FROM weather)
            TO '{s3_path}'
            (FORMAT 'parquet', COMPRESSION 'zstd',
            PARTITION_BY ({partition_by}),
            OVERWRITE_OR_IGNORE true);
            """
        )
        conn.sql(sql)
        conn.close()

    def _get_station_bounds(self):
        combined_bbox = Station.objects.filter(
            provider=self.dataset.provider
        ).aggregate(
            combined_geometry=Union('geometry')
        )
        return combined_bbox['combined_geometry'].extent

    def _get_station_df(self, year, month=None):
        station_ids = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year
        )

        station_ids = station_ids.distinct('station_id').values_list(
            'station_id',
            flat=True
        )

        stations = Station.objects.filter(
            id__in=station_ids
        ).order_by('id')
        fields = {
            'loc_x': ST_X('geometry'),
            'loc_y': ST_Y('geometry'),
            'st_id': F('id'),
            'st_code': F('code'),
            'iso_a3': F('country__iso_a3'),
            'geom': AsWKB('geometry'),
        }
        stations = stations.annotate(**fields).values(
            *(list(fields.keys()) + ['altitude', 'country_id'])
        )
        df = pd.DataFrame(list(stations))
        df['altitude'] = df['altitude'].astype('double')
        return df

    def _process_weather_df(self, year: int, measurements: list, month=None):
        # Convert to DataFrame
        df = pd.DataFrame(measurements)
        # Pivot the data to make attributes as columns
        df = df.pivot_table(
            index=[
                k for k in list(self.WEATHER_FIELDS.keys()) if k != 'attr'
            ],
            columns='attr',
            values='value'
        ).reset_index()
        print(f'Year {year} after pivot total_count: {df.shape[0]}')
        # rename date time column
        df = df.rename(columns={'dt': 'date_time'})

        # add missing attributes
        missing_cols = []
        for attribute in self.attributes:
            if attribute in df.columns:
                continue
            missing_cols.append(
                pd.Series(
                    np.nan, dtype='double', index=df.index, name=attribute
                )
            )
        if missing_cols:
            print(f'Adding missing columns: {len(missing_cols)}')
            missing_cols.insert(0, df)
            df = pd.concat(missing_cols, axis=1)

        station_df = self._get_station_df(year, month=month)
        # merge df with station_df
        return df.merge(station_df, on=[self.STATION_JOIN_KEY], how='inner')

    def _process_subset(self, year: int, month=None):
        measurements = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year
        )
        if month:
            measurements = measurements.filter(
                date_time__month=month
            )

        measurements = measurements.order_by('date_time', 'station_id')
        print(f'Year {year} total_count: {measurements.count()}')

        if measurements.count() == 0:
            return None

        measurements = measurements.annotate(**self.WEATHER_FIELDS).values(
            *(list(self.WEATHER_FIELDS.keys()) + ['value'])
        )

        return self._process_weather_df(year, list(measurements), month=month)

    def run(self):
        """Run the converter."""
        s3_path = self._get_directory_path(self.data_source)

        # get all distinct years
        years = list(Measurement.objects.annotate(
            year=ExtractYear('date_time')
        ).filter(
            dataset_attribute__dataset=self.dataset
        ).order_by('year').distinct('year').values_list(
            'year',
            flat=True
        ))

        station_bbox = self._get_station_bounds()
        for year in years:
            parquet_exists = self._check_parquet_exists(s3_path, year)
            df = self._process_subset(year)

            if df is None:
                continue

            if self.mode == 'a' and parquet_exists:
                self._append_dataframe_to_geoparquet(
                    df, s3_path, station_bbox, year
                )
            else:
                self._store_dataframe_as_geoparquet(df, s3_path, station_bbox)


class WindborneParquetConverter(ParquetConverter):
    """Class to convert WindborneMeasurement data to GeoParquet."""

    STATION_JOIN_KEY = 'st_hist_id'
    WEATHER_FIELDS = {
        'dt': F('date_time'),
        'attr': F('dataset_attribute__attribute__variable_name'),
        'st_id': F('station__id'),
        'st_hist_id': F('station_history__id'),
        'year': (
            ExtractYear('date_time')
        ),
        'month': (
            ExtractMonth('date_time')
        ),
        'day': (
            ExtractDay('date_time')
        ),
    }

    def __init__(self, dataset, data_source, mode='w'):
        """Initialize WindborneParquetConverter."""
        super().__init__(dataset, data_source, mode)

    def _get_station_bounds(self):
        combined_bbox = StationHistory.objects.filter(
            station__provider=self.dataset.provider
        ).aggregate(
            combined_geometry=Union('geometry')
        )
        return combined_bbox['combined_geometry'].extent

    def _get_station_df(self, year, month=None):
        station_hist_ids = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year,
            date_time__month=month
        ).distinct('station_history_id').values_list(
            'station_history_id',
            flat=True
        )

        stations = StationHistory.objects.filter(
            id__in=station_hist_ids
        ).order_by('id')
        fields = {
            'st_hist_id': F('id'),
            'loc_x': ST_X('geometry'),
            'loc_y': ST_Y('geometry'),
            'st_code': F('station__code'),
            'iso_a3': F('station__country__iso_a3'),
            'geom': AsWKB('geometry'),
            'country_id': F('station__country_id')
        }
        stations = stations.annotate(**fields).values(
            *(list(fields.keys()) + ['st_hist_id', 'altitude'])
        )
        df = pd.DataFrame(list(stations))
        df['altitude'] = df['altitude'].astype('double')
        return df

    def run(self):
        """Run the converter."""
        s3_path = self._get_directory_path(self.data_source)

        # get all distinct year and months
        months = list(Measurement.objects.annotate(
            month=TruncMonth('date_time')
        ).filter(
            dataset_attribute__dataset=self.dataset
        ).order_by('month').distinct('month').values_list(
            'month',
            flat=True
        ))

        station_bbox = self._get_station_bounds()
        for month_year in months:
            year = month_year.year
            month = month_year.month
            parquet_exists = self._check_parquet_exists(
                s3_path, year, month=month
            )
            df = self._process_subset(year, month=month)

            if df is None:
                continue

            if self.mode == 'a' and parquet_exists:
                self._append_dataframe_to_geoparquet(
                    df, s3_path, station_bbox, year, month=month
                )
            else:
                self._store_dataframe_as_geoparquet(
                    df, s3_path, station_bbox, use_month=True
                )


class ParquetIngestorAppender(ParquetConverter):
    """Class to append data to parquet from Ingestor."""

    def __init__(
        self, dataset: Dataset, data_source: DataSourceFile,
        start_date: datetime, end_date: datetime, mode='a'
    ):
        """Initialize ParquetIngestorAppender."""
        super().__init__(dataset, data_source, mode)
        self.start_date = start_date
        self.end_date = end_date

    def _process_date_range(
        self, year: int, start_date: datetime, end_date: datetime
    ):
        measurements = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year,
            date_time__gte=start_date,
            date_time__lte=end_date
        ).order_by('date_time', 'station_id')
        print(
            f'{start_date} to {end_date} total_count: {measurements.count()}'
        )

        if measurements.count() == 0:
            return None

        measurements = measurements.annotate(**self.WEATHER_FIELDS).values(
            *(list(self.WEATHER_FIELDS.keys()) + ['value'])
        )

        return self._process_weather_df(year, list(measurements))

    def run(self):
        """Run the converter."""
        if self.start_date is None or self.end_date is None:
            return
        s3_path = self._get_directory_path(self.data_source)
        date_list = split_epochs_by_year(
            int(self.start_date.timestamp()),
            int(self.end_date.timestamp()),
        )

        station_bbox = self._get_station_bounds()
        for year, start_epoch, end_epoch in date_list:
            start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(end_epoch, tz=timezone.utc)

            parquet_exists = self._check_parquet_exists(s3_path, year)
            df = self._process_date_range(year, start_dt, end_dt)

            if df is None:
                continue

            if self.mode == 'a' and parquet_exists:
                self._append_dataframe_to_geoparquet(
                    df, s3_path, station_bbox, year
                )
            else:
                self._store_dataframe_as_geoparquet(df, s3_path, station_bbox)


class WindborneParquetIngestorAppender(WindborneParquetConverter):
    """Class to append data to parquet from Ingestor."""

    def __init__(
        self, dataset: Dataset, data_source: DataSourceFile,
        start_date: datetime, end_date: datetime, mode='a'
    ):
        """Initialize ParquetIngestorAppender."""
        super().__init__(dataset, data_source, mode)
        self.start_date = start_date
        self.end_date = end_date

    def _process_date_range(
        self, year: int, start_date: datetime, end_date: datetime
    ):
        measurements = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year,
            date_time__gte=start_date,
            date_time__lte=end_date
        ).order_by('date_time', 'station_id')
        print(
            f'{start_date} to {end_date} total_count: {measurements.count()}'
        )

        if measurements.count() == 0:
            return None

        measurements = measurements.annotate(**self.WEATHER_FIELDS).values(
            *(list(self.WEATHER_FIELDS.keys()) + ['value'])
        )

        return self._process_weather_df(
            year, list(measurements), month=start_date.month
        )

    def run(self):
        """Run the converter."""
        if self.start_date is None or self.end_date is None:
            return
        s3_path = self._get_directory_path(self.data_source)
        date_list = split_epochs_by_year_month(
            int(self.start_date.timestamp()),
            int(self.end_date.timestamp()),
        )

        station_bbox = self._get_station_bounds()
        for year, month, start_epoch, end_epoch in date_list:
            start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(end_epoch, tz=timezone.utc)

            parquet_exists = self._check_parquet_exists(
                s3_path, year, month=month
            )
            df = self._process_date_range(year, start_dt, end_dt)

            if df is None:
                continue

            if self.mode == 'a' and parquet_exists:
                self._append_dataframe_to_geoparquet(
                    df, s3_path, station_bbox, year, month=month
                )
            else:
                self._store_dataframe_as_geoparquet(
                    df, s3_path, station_bbox, use_month=True
                )
