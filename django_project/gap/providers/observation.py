# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Observation Data Reader
"""

import os
import json
import duckdb
from _collections_abc import dict_values
from datetime import datetime
import pandas as pd
import tempfile
from django.db.models import Exists, OuterRef, F, FloatField, QuerySet
from django.db.models.functions.datetime import TruncDate, TruncTime
from django.contrib.gis.geos import Polygon, Point
from django.contrib.gis.db.models.functions import Distance, GeoFunc
from typing import List, Tuple, Union
from django.core.files.storage import storages
from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement,
    Preferences
)
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    BaseDatasetReader,
    DatasetReaderValue
)
from gap.utils.dask import execute_dask_compute


class ST_X(GeoFunc):
    """Custom GeoFunc to extract lon."""

    output_field = FloatField()
    function = 'ST_X'


class ST_Y(GeoFunc):
    """Custom GeoFunc to extract lat."""

    output_field = FloatField()
    function = 'ST_Y'


class CSVBuffer:
    """An object that implements the write method of file-like interface."""

    def write(self, value):
        """Return the string to write."""
        yield value


class ObservationReaderValue(DatasetReaderValue):
    """Class that convert Dataset to TimelineValues."""

    date_variable = 'date'

    def __init__(
            self, val: QuerySet,
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            start_date: datetime,
            end_date: datetime,
            nearest_stations,
            result_count) -> None:
        """Initialize ObservationReaderValue class.

        :param val: value that has been read
        :type val: List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(
            val, location_input, attributes,
            result_count=result_count
        )
        self.start_date = start_date
        self.end_date = end_date
        self.nearest_stations = nearest_stations
        self.attributes = sorted(
            self.attributes, key=lambda x: x.attribute.id
        )

    def _get_data_frame(
            self, use_separate_time_col=True,
            use_station_id=False) -> pd.DataFrame:
        """Create a dataframe from query result.

        :return: Data frame
        :rtype: pd.DataFrame
        """
        fields = {
            'date': (
                TruncDate('date_time') if use_separate_time_col else
                F('date_time')
            ),
            'loc_x': ST_X('geom'),
            'loc_y': ST_Y('geom'),
            'attr_id': F('dataset_attribute__attribute__id'),
        }
        field_index = [
            'date'
        ]

        # add time if time_step is not daily
        if self.has_time_column and use_separate_time_col:
            fields.update({
                'time': TruncTime('date_time')
            })
            field_index.append('time')

        # add lat and lon
        field_index.extend(['loc_y', 'loc_x'])

        # add altitude if it's upper air observation
        if self.has_altitude_column:
            fields.update({
                'loc_alt': F('alt')
            })
            field_index.append('loc_alt')

        # add station id if needed
        if use_station_id:
            fields.update({
                's_id': F('station__code')
            })
            field_index.append('s_id')

        # annotate and select required fields only
        measurements = self._val.annotate(**fields).values(
            *(list(fields.keys()) + ['value'])
        )
        # Convert to DataFrame
        df = pd.DataFrame(list(measurements))

        # Pivot the data to make attributes columns
        df = df.pivot_table(
            index=field_index,
            columns='attr_id',
            values='value'
        ).reset_index()

        # add other attributes
        for attr in self.attributes:
            if attr.attribute.id not in df.columns:
                df[attr.attribute.id] = None

        # reorder columns
        df = df[
            field_index + [attr.attribute.id for attr in self.attributes]
        ]

        return df

    def _get_headers(self, use_separate_time_col=True, use_station_id=False):
        """Get list of headers that allign with dataframce columns."""
        headers = ['date']

        # add time if time_step is not daily
        if self.has_time_column and use_separate_time_col:
            headers.append('time')

        # add lat and lon
        headers.extend(['lat', 'lon'])

        # add altitude if it's upper air observation
        if self.has_altitude_column:
            headers.append('altitude')

        # add station_id
        if use_station_id:
            headers.append('station_id')

        field_indices = [header for header in headers]

        # add headers
        for attr in self.attributes:
            headers.append(attr.attribute.variable_name)

        return headers, field_indices

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream.

        :param suffix: file extension, defaults to '.csv'
        :type suffix: str, optional
        :param separator: separator, defaults to ','
        :type separator: str, optional
        :yield: bytes of csv file
        :rtype: bytes
        """
        headers, _ = self._get_headers(use_station_id=True)
        # get dataframe
        df_pivot = self._get_data_frame(use_station_id=True)

        # write headers
        yield bytes(separator.join(headers) + '\n', 'utf-8')

        # Write the data in chunks
        for start in range(0, len(df_pivot), self.csv_chunk_size):
            chunk = df_pivot.iloc[start:start + self.csv_chunk_size]
            yield chunk.to_csv(
                index=False, header=False, float_format='%g',
                sep=separator
            )

    def to_csv(self, suffix='.csv', separator=','):
        """Generate csv file to object storage."""
        headers, _ = self._get_headers(use_station_id=True)

        # get dataframe
        df_pivot = self._get_data_frame(use_station_id=True)

        output = self._get_file_remote_url(suffix)
        s3_storage: S3Boto3Storage = storages["gap_products"]

        with (
            tempfile.NamedTemporaryFile(
                suffix=suffix, delete=True, delete_on_close=False)
        ) as tmp_file:
            # write headers
            write_headers = True

            # Write the data in chunks
            for start in range(0, len(df_pivot), self.csv_chunk_size):
                chunk = df_pivot.iloc[start:start + self.csv_chunk_size]
                if write_headers:
                    chunk.columns = headers

                chunk.to_csv(
                    tmp_file.name, index=False, header=write_headers,
                    float_format='%g',
                    sep=separator, mode='a'
                )

                if write_headers:
                    write_headers = False

            # upload to s3
            s3_storage.transfer_config = (
                Preferences.user_file_s3_transfer_config()
            )
            s3_storage.save(output, tmp_file)

        return output

    def _get_xarray_dataset(self):
        time_col_exists = self.has_time_column

        # if time column exists, in netcdf we should use datetime
        # instead of separating the date and time columns
        headers, field_indices = self._get_headers(
            use_separate_time_col=not time_col_exists
        )

        # get dataframe
        df_pivot = self._get_data_frame(
            use_separate_time_col=not time_col_exists
        )

        # rename columns
        if time_col_exists:
            headers[0] = 'time'
            field_indices[0] = 'time'
        df_pivot.columns = headers

        # convert date/datetime objects
        date_coord = 'date' if not time_col_exists else 'time'
        df_pivot[date_coord] = pd.to_datetime(df_pivot[date_coord])

        # Convert to xarray Dataset
        df_pivot.set_index(field_indices, inplace=True)

        return df_pivot.to_xarray()

    def to_netcdf_stream(self):
        """Generate NetCDF."""
        ds = self._get_xarray_dataset()

        # write to netcdf
        with (
            tempfile.NamedTemporaryFile(
                suffix=".nc", delete=True, delete_on_close=False)
        ) as tmp_file:
            x = ds.to_netcdf(
                tmp_file.name, format='NETCDF4', engine='netcdf4',
                compute=False
            )
            execute_dask_compute(x, is_api=True)
            with open(tmp_file.name, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk

    def to_netcdf(self):
        """Generate netcdf file to object storage."""
        ds = self._get_xarray_dataset()
        output_url = self._get_file_remote_url('.nc')
        s3_storage: S3Boto3Storage = storages["gap_products"]
        with (
            tempfile.NamedTemporaryFile(
                suffix=".nc", delete=True, delete_on_close=False)
        ) as tmp_file:
            x = ds.to_netcdf(
                tmp_file.name, format='NETCDF4', engine='h5netcdf',
                compute=False
            )
            execute_dask_compute(x)

            # upload to s3
            s3_storage.transfer_config = (
                Preferences.user_file_s3_transfer_config()
            )
            s3_storage.save(output_url, tmp_file)

        return output_url

    def _to_dict(self) -> dict:
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        if (
            self.location_input is None or self._val is None or
            self.count() == 0
        ):
            return {}

        has_altitude = self.has_altitude_column
        output = {
            'geometry': json.loads(self.location_input.geometry.json),
            'station_id': '',
            'data': []
        }

        # get dataframe
        df_pivot = self._get_data_frame(
            use_separate_time_col=False,
            use_station_id=True
        )
        for _, row in df_pivot.iterrows():
            values = {}
            for attr in self.attributes:
                values[attr.attribute.variable_name] = row[attr.attribute.id]
            output['data'].append({
                'datetime': row['date'].isoformat(timespec='seconds'),
                'values': values
            })

            if has_altitude:
                output['altitude'] = row['loc_alt']

            output['station_id'] = row['s_id']

        return output


class ObservationDatasetReader(BaseDatasetReader):
    """Class to read observation ground observation data."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: Tuple[float, float] = None
    ) -> None:
        """Initialize ObservationDatasetReader class.

        :param dataset: Dataset from observation provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.results: QuerySet = QuerySet.none
        self.result_count = 0
        self.nearest_stations = None

    def _get_count(self, values: Union[List, QuerySet]) -> int:
        """Get count of a list of queryset.

        :param values: List or QuerySet object
        :type values: Union[List, QuerySet]
        :return: count
        :rtype: int
        """
        if isinstance(values, (list, dict_values,)):
            return len(values)
        return values.count()

    def query_by_altitude(self, qs):
        """Query by altitude."""
        altitudes = self.altitudes
        try:
            if altitudes[0] is not None and altitudes[1] is not None:
                qs = qs.filter(
                    altitude__gte=altitudes[0]
                ).filter(
                    altitude__lte=altitudes[1]
                )
        except (IndexError, TypeError):
            pass
        return qs

    def _find_nearest_station_by_point(self, point: Point = None):
        p = point
        if p is None:
            p = self.location_input.point
        # has_measurement is for removing duplicates station
        qs = Station.objects.annotate(
            distance=Distance('geometry', p),
            has_measurement=Exists(
                Measurement.objects.filter(
                    station=OuterRef('pk'),
                    dataset_attribute__dataset=self.dataset
                )
            )
        ).filter(
            provider=self.dataset.provider,
            has_measurement=True
        ).order_by('distance').first()
        if qs is None:
            return None
        return [qs]

    def _find_nearest_station_by_bbox(self):
        points = self.location_input.points
        polygon = Polygon.from_bbox(
            (points[0].x, points[0].y, points[1].x, points[1].y))
        qs = Station.objects.filter(
            geometry__within=polygon
        ).order_by('id')
        return qs

    def _find_nearest_station_by_polygon(self):
        qs = Station.objects.filter(
            geometry__within=self.location_input.polygon
        ).order_by('id')
        return qs

    def _find_nearest_station_by_points(self):
        points = self.location_input.points
        results = {}
        for point in points:
            rs = self._find_nearest_station_by_point(point)
            if rs is None:
                continue
            if rs[0].id in results:
                continue
            results[rs[0].id] = rs[0]
        return results.values()

    def get_nearest_stations(self):
        """Return nearest stations."""
        nearest_stations = None
        if self.location_input.type == LocationInputType.POINT:
            nearest_stations = self._find_nearest_station_by_point()
        elif self.location_input.type == LocationInputType.POLYGON:
            nearest_stations = self._find_nearest_station_by_polygon()
        elif self.location_input.type == LocationInputType.LIST_OF_POINT:
            nearest_stations = self._find_nearest_station_by_points()
        elif self.location_input.type == LocationInputType.BBOX:
            nearest_stations = self._find_nearest_station_by_bbox()
        return nearest_stations

    def get_measurements(self, start_date: datetime, end_date: datetime):
        """Return measurements data."""
        self.nearest_stations = self.get_nearest_stations()
        if (
            self.nearest_stations is None or
            self._get_count(self.nearest_stations) == 0
        ):
            return Measurement.objects.none()

        return Measurement.objects.annotate(
            geom=F('station__geometry'),
            alt=F('station__altitude')
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station__in=self.nearest_stations
        ).order_by('date_time')

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        measurements = self.get_measurements(start_date, end_date)
        self.result_count = measurements.count()
        if measurements is None or self.result_count == 0:
            return

        self.results = measurements

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return ObservationReaderValue(
            self.results, self.location_input, self.attributes,
            self.start_date, self.end_date, self.nearest_stations,
            self.result_count
        )


class TahmoParquetReaderValue(DatasetReaderValue):
    """Class to generate the query output from Parquet files."""

    def __init__(
            self, val: duckdb.DuckDBPyConnection,
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            start_date: datetime,
            end_date: datetime,
            query) -> None:
        """Initialize TahmoParquetReaderValue class.

        :param val: value that has been read
        :type val: List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(
            val, location_input, attributes, result_count=1
        )
        self.start_date = start_date
        self.end_date = end_date
        self.attributes = sorted(
            self.attributes, key=lambda x: x.attribute.id
        )
        self.query = query

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB Connection."""
        return self._val

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream.

        :param suffix: file extension, defaults to '.csv'
        :type suffix: str, optional
        :param separator: separator, defaults to ','
        :type separator: str, optional
        :yield: bytes of csv file
        :rtype: bytes
        """
        with (
            tempfile.NamedTemporaryFile(
                suffix=suffix, delete=True, delete_on_close=False)
        ) as tmp_file:
            export_query = (
                f"""
                    COPY({self.query})
                    TO '{tmp_file.name}'
                    (HEADER, DELIMITER '{separator}');
                """
            )

            self.conn.sql(export_query)
            self.conn.close()

            with open(tmp_file.name, 'r') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk


class TahmoParquetReader(ObservationDatasetReader):
    """Class to read tahmo dataset in GeoParquet."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: Tuple[float, float] = None
    ) -> None:
        """Initialize TahmoParquetReader class.

        :param dataset: Dataset from observation provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.s3 = self._get_s3_variables()

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
        if settings.DEBUG:
            results['AWS_ENDPOINT_URL'] = results['AWS_ENDPOINT_URL'].replace(
                'http://', ''
            )
        else:
            results['AWS_ENDPOINT_URL'] = results['AWS_ENDPOINT_URL'].replace(
                'https://', ''
            )
        if results['AWS_ENDPOINT_URL'].endswith('/'):
            results['AWS_ENDPOINT_URL'] = results['AWS_ENDPOINT_URL'][:-1]
        return results

    def _get_directory_path(self):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/tahmo/"
        )

    def _get_connection(self):
        conn = duckdb.connect(config={
            'enable_object_cache': True,
            's3_access_key_id': self.s3['AWS_ACCESS_KEY_ID'],
            's3_secret_access_key': self.s3['AWS_SECRET_ACCESS_KEY'],
            's3_region': 'us-east-1',
            's3_url_style': 'path',
            's3_endpoint': self.s3['AWS_ENDPOINT_URL'],
            's3_use_ssl': not settings.DEBUG
        })
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        return conn


    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        if self.location_input.type != LocationInputType.BBOX:
            raise NotImplementedError('Only BBOX is supported!')

        attributes = ', '.join(
            [a.attribute.variable_name for a in self.attributes]
        )
        s3_path = self._get_directory_path()
        points = self.location_input.points
        self.query = (
            f"""
            SELECT date, loc_y as lat, loc_x as lon, st_code as station_id,
            {attributes}
            FROM read_parquet('{s3_path}country=*/year=*/month=*/*.parquet',
            hive_partitioning=true)
            WHERE year>={start_date.year} AND month>={start_date.month} AND
            day>={start_date.day} AND
            year<={end_date.year} AND month<={end_date.month} AND
            day<={end_date.day} AND
            ST_Within(ST_GeomFromWKB(geometry), ST_MakeEnvelope(
            {points[0].x}, {points[0].y}, {points[1].x}, {points[1].y}))
            ORDER BY date
            """
        )

        # nearest_stations = self._find_nearest_station_by_bbox()
        # station_id_list = [s.id for s in nearest_stations]
        # self.query = (
        #     f"""
        #     SELECT date, loc_y as lat, loc_x as lon, st_code as station_id,
        #     {attributes}
        #     FROM read_parquet('{s3_path}country=*/year=*/month=*/*.parquet',
        #     hive_partitioning=true)
        #     WHERE year>={start_date.year} AND month>={start_date.month} AND
        #     day>={start_date.day} AND
        #     year<={end_date.year} AND month<={end_date.month} AND
        #     day<={end_date.day} AND
        #     st_id IN {station_id_list}
        #     ORDER BY date
        #     """
        # )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return TahmoParquetReaderValue(
            self._get_connection(), self.location_input, self.attributes,
            self.start_date, self.end_date, self.query
        )
