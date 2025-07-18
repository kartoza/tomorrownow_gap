# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading dataset
"""

import os
import json
import tempfile
import dask
import uuid
import pandas as pd
from functools import cached_property
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Union, List, Tuple

import numpy as np
import pytz
from django.db.models import QuerySet
from django.contrib.gis.geos import (
    Point, Polygon, MultiPolygon, GeometryCollection, MultiPoint, GEOSGeometry
)
from xarray.core.dataset import Dataset as xrDataset

from core.models import ObjectStorageManager
from gap.models import (
    CastType,
    Attribute,
    Unit,
    Dataset,
    DatasetAttribute,
    DatasetTimeStep,
    DatasetObservationType,
    Preferences
)
from gap.utils.dask import execute_dask_compute, get_num_of_threads


class DatasetVariable:
    """Contains Variable from a Dataset."""

    def __init__(
            self, name, desc, unit, attr_var_name=None) -> None:
        """Initialize variable object.

        :param name: Name of the variable
        :type name: str
        :param desc: Description of the variable
        :type desc: str
        :param unit: Unit
        :type unit: str, optional
        :param attr_var_name: Mapping to attribute name, defaults to None
        :type attr_var_name: str, optional
        """
        self.name = name
        self.desc = desc
        self.unit = unit
        self.attr_var_name = attr_var_name

    def get_gap_attribute(self) -> Attribute:
        """Get or create a mapping attribute.

        :return: Gap Attribute
        :rtype: Attribute
        """
        if self.attr_var_name is None:
            return None
        unit, _ = Unit.objects.get_or_create(
            name=self.unit
        )
        attr, _ = Attribute.objects.get_or_create(
            variable_name=self.attr_var_name,
            defaults={
                'description': self.desc,
                'name': self.name,
                'unit': unit,
            }
        )
        return attr


class LocationInputType:
    """Class for data input type."""

    POINT = 'point'
    BBOX = 'bbox'
    POLYGON = 'polygon'
    LIST_OF_POINT = 'list_of_point'

    @staticmethod
    def map_from_geom_typeid(geom_typeid: int) -> str:
        """Get type from geom_typeid.

        :param geom_typeid: id from geom_typeid
        :type geom_typeid: int
        :return: LocationInputType
        :rtype: str
        """
        if geom_typeid == 0:
            return LocationInputType.POINT
        elif geom_typeid in [3, 6]:
            return LocationInputType.POLYGON
        elif geom_typeid == 4:
            return LocationInputType.LIST_OF_POINT


class DatasetReaderInput:
    """Class to store the dataset reader input.

    Input type: Point, bbox, polygon, list of point
    """

    def __init__(self, geom_collection: GeometryCollection, type: str):
        """Initialize DatasetReaderInput class."""
        self.geom_collection = geom_collection
        self.type = type

    @property
    def point(self) -> Point:
        """Get single point from input."""
        if self.type != LocationInputType.POINT:
            raise TypeError('Location input type is not point!')
        return Point(
            x=self.geom_collection[0].x,
            y=self.geom_collection[0].y, srid=4326)

    @property
    def polygon(self) -> MultiPolygon:
        """Get MultiPolygon object from input."""
        if self.type != LocationInputType.POLYGON:
            raise TypeError('Location input type is not polygon!')
        return self.geom_collection

    @property
    def points(self) -> List[Point]:
        """Get list of point from input."""
        if self.type not in [
            LocationInputType.BBOX, LocationInputType.LIST_OF_POINT
        ]:
            raise TypeError('Location input type is not bbox/points!')
        return [
            Point(x=point.x, y=point.y, srid=4326) for
            point in self.geom_collection
        ]

    @classmethod
    def from_point(cls, point: Point):
        """Create input from single point.

        :param point: single point
        :type point: Point
        :return: DatasetReaderInput with POINT type
        :rtype: DatasetReaderInput
        """
        return DatasetReaderInput(
            MultiPoint([point]), LocationInputType.POINT)

    @classmethod
    def from_polygon(cls, polygon: Polygon):
        """Create input from single point.

        :param polygon: single polygon
        :type polygon: Polygon
        :return: DatasetReaderInput with Polygon type
        :rtype: DatasetReaderInput
        """
        return DatasetReaderInput(
            MultiPolygon([polygon]), LocationInputType.POLYGON
        )

    @classmethod
    def from_bbox(cls, bbox_list: List[float]):
        """Create input from bbox (xmin, ymin, xmax, ymax).

        :param bbox_list: (xmin, ymin, xmax, ymax)
        :type bbox_list: List[float]
        :return: DatasetReaderInput with BBOX type
        :rtype: DatasetReaderInput
        """
        return DatasetReaderInput(
            MultiPoint([
                Point(x=bbox_list[0], y=bbox_list[1], srid=4326),
                Point(x=bbox_list[2], y=bbox_list[3], srid=4326)
            ]), LocationInputType.BBOX)

    @classmethod
    def from_list_of_points(cls, points: List[Tuple[float, float]]):
        """Create DatasetReaderInput from a list of points.

        :param points: List of tuples with (lat, lon) coordinates
        :type points: List[Tuple[float, float]]
        :return: DatasetReaderInput instance
        :rtype: DatasetReaderInput
        """
        point_objects = [
            Point(lon, lat) for lat, lon in points
        ]
        return DatasetReaderInput(
            point_objects, LocationInputType.LIST_OF_POINT,
        )

    @property
    def geometry(self) -> GEOSGeometry:
        """Return geometry of geom_collection."""
        geometry = self.geom_collection
        if self.type == LocationInputType.POINT:
            geometry = self.point
        elif self.type == LocationInputType.POLYGON:
            geometry = self.geom_collection[0]
        return geometry


class DatasetReaderOutputType:
    """Dataset Output Type Format."""

    JSON = 'json'
    NETCDF = 'netcdf'
    CSV = 'csv'
    ASCII = 'ascii'


class DatasetTimelineValue:
    """Class representing data value for given datetime."""

    def __init__(
            self, datetime: Union[np.datetime64, datetime],
            values: dict, location: Point, altitude: int = None
    ) -> None:
        """Initialize DatasetTimelineValue object.

        :param datetime: datetime of data
        :type datetime: np.datetime64 or datetime
        :param values: Dictionary of variable and its value
        :type values: dict
        """
        self.datetime = datetime
        self.values = values
        self.location = location
        self.altitude = altitude

    def _datetime_as_str(self):
        """Convert datetime object to string."""
        if self.datetime is None:
            return ''
        if isinstance(self.datetime, np.datetime64):
            return np.datetime_as_string(
                self.datetime, unit='s', timezone='UTC')
        return self.datetime.isoformat(timespec='seconds')

    def get_datetime(self) -> datetime:
        """Get datetime value.

        :return: parsed datetime
        :rtype: datetime
        """
        dt = self.datetime
        if isinstance(self.datetime, np.datetime64):
            timestamp = (
                    (dt - np.datetime64('1970-01-01T00:00:00')) /
                    np.timedelta64(1, 's')
            )
            dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        return dt

    def get_datetime_repr(self, format: str) -> str:
        """Return the representation of datetime in given format.

        :param format: Format like '%Y-%m-%d'
        :type format: str
        :return: String of datetime
        :rtype: str
        """
        return self.get_datetime().strftime(format)

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of datetime and values
        :rtype: dict
        """
        return {
            'datetime': self._datetime_as_str(),
            'values': self.values
        }


class DatasetReaderValue:
    """Class that represents the value after reading dataset."""

    date_variable = 'date'
    chunk_size_in_bytes = 81920  # 80KB chunks
    csv_chunk_size = 50000

    def __init__(
        self, val: Union[xrDataset, List[DatasetTimelineValue], QuerySet],
        location_input: DatasetReaderInput,
        attributes: List[DatasetAttribute],
        result_count = None,
        start_datetime: np.datetime64 = None,
        end_datetime: np.datetime64 = None
    ) -> None:
        """Initialize DatasetReaderValue class.

        :param val: value that has been read
        :type val: Union[xrDataset, List[DatasetTimelineValue], QuerySet]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        self._val = val
        self._is_xr_dataset = isinstance(val, xrDataset)
        self.location_input = location_input
        self.attributes = attributes
        self._result_count = result_count
        self.output_metadata = {
            'size': 0
        }
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self._post_init()

    def _post_init(self):
        """Rename source variable into attribute name."""
        if not self._is_xr_dataset:
            return
        if self.is_empty():
            return
        renamed_dict = {}
        for attr in self.attributes:
            renamed_dict[attr.source] = attr.attribute.variable_name
        self._val = self._val.rename(renamed_dict)

    def _get_dataset(self) -> Dataset:
        """Get dataset from attribute.

        :return: dataset object
        :rtype: Dataset
        """
        if len(self.attributes) > 0:
            return self.attributes[0].dataset

        return None

    @property
    def size(self) -> int:
        """Get size of the dataset.

        :return: size of the dataset
        :rtype: int
        """
        if self._is_xr_dataset:
            # estimate size of dataset
            return self._val.nbytes
        if isinstance(self._val, list):
            # for list of dataset timeline value
            return len(self._val)
        return 0

    @property
    def xr_dataset(self) -> xrDataset:
        """Return the value as xarray Dataset.

        :return: xarray dataset object
        :rtype: xrDataset
        """
        return self._val

    @property
    def values(self) -> List[DatasetTimelineValue]:
        """Return the value as list of dataset timeline value.

        :return: list values
        :rtype: List[DatasetTimelineValue]
        """
        return self._val

    @cached_property
    def has_time_column(self) -> bool:
        """Check if the output has time column.

        :return: True if time column should exist
        :rtype: bool
        """
        dataset = self._get_dataset()

        return (
            dataset.time_step != DatasetTimeStep.DAILY if
            dataset else False
        )

    @cached_property
    def has_altitude_column(self) -> bool:
        """Check if the output has altitude column.

        :return: True if altitude column should exist
        :rtype: bool
        """
        dataset = self._get_dataset()

        return (
            dataset.observation_type ==
            DatasetObservationType.UPPER_AIR_OBSERVATION if
            dataset else False
        )

    def count(self):
        """Return the count for QuerySet."""
        if self._result_count is not None:
            return self._result_count
        return len(self.values)

    def is_empty(self) -> bool:
        """Check if value is empty.

        :return: True if empty dataset or empty list
        :rtype: bool
        """
        if self._val is None:
            return True

        return self.count() == 0

    def _to_dict(self) -> dict:
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        if (
            self.location_input is None or self._val is None or
            len(self.values) == 0
        ):
            return {}

        altitude = None
        try:
            altitude = self.values[0].altitude
        except IndexError:
            pass

        output = {
            'geometry': json.loads(self.location_input.geometry.json),
        }
        if altitude is not None:
            output['altitude'] = altitude
        output['data'] = [result.to_dict() for result in self.values]
        return output

    def _xr_dataset_process_datetime(self, df):
        """Process datetime for df from xarray dataset."""
        # add datetime column
        if self.has_time_column:
            df['datetime'] = pd.to_datetime(
                df[self.date_variable].astype(str) + ' ' +
                df['time'].astype(str)
            )
            df = df.drop(columns=['time', self.date_variable])
        else:
            df['datetime'] = pd.to_datetime(
                df[self.date_variable].astype(str)
            )
            df = df.drop(columns=[self.date_variable])
        return df

    def _xr_dataset_to_dict(self) -> dict:
        """Convert xArray Dataset to dictionary.

        Implementation depends on provider.
        :return: data dictionary
        :rtype: dict
        """
        if self.is_empty():
            return {
                'geometry': json.loads(self.location_input.point.json),
                'data': []
            }
        ds, dim_order, reordered_cols = self._get_dataset_for_csv()
        df = ds.to_dataframe(dim_order=dim_order)
        df = df[reordered_cols]
        df = df.drop(columns=['lat', 'lon'])
        df = df.reset_index()
        # Replace NaN with None
        df = df.astype(object).where(pd.notnull(df), None)

        # add datetime column
        df = self._xr_dataset_process_datetime(df)

        df = self._filter_df(df)
        if 'ensemble' in df.columns:
            # Sort first to ensure ensemble order is preserved
            df = df.sort_values(['datetime', 'ensemble'])
            # Group by datetime, then aggregate the values:
            # - as lists for ensemble attributes
            # - as first value for non-ensemble attributes
            df = df.groupby('datetime').agg({
                **{
                    col.attribute.variable_name: list
                    for col in self.attributes if col.ensembles
                },
                **{
                    col.attribute.variable_name: 'first'
                    for col in self.attributes if col.ensembles is False
                }
            })
            df = df.reset_index()
        return {
            'geometry': json.loads(self.location_input.point.json),
            'data': df.to_dict(orient='records')
        }

    def to_json(self) -> dict:
        """Convert result to json.

        :raises TypeError: if location input is not a Point
        :return: data dictionary
        :rtype: dict
        """
        if self.location_input.type not in [
            LocationInputType.POINT, LocationInputType.POLYGON
        ]:
            raise TypeError('Location input type is not point or polygon!')
        if self._is_xr_dataset:
            return self._xr_dataset_to_dict()
        return self._to_dict()

    def _get_s3_variables(self) -> dict:
        """Get s3 env variables for product bucket.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        results = ObjectStorageManager.get_s3_env_vars()
        return results

    def _get_file_remote_url(self, suffix):
        # s3 variables to product bucket
        s3 = self._get_s3_variables()

        output_url = s3["S3_DIR_PREFIX"]
        if not output_url.endswith('/'):
            output_url += '/'
        output_url += f'user_data/{uuid.uuid4().hex}{suffix}'

        return output_url

    def _get_remote_file_path(self, suffix):
        output_url = f'user_data/{uuid.uuid4().hex}{suffix}'
        return output_url

    def _upload_to_s3(self, tmp_file_path: str, suffix: str):
        """Upload file to S3."""
        remote_file_path = self._get_remote_file_path(suffix)
        transfer_config = Preferences.user_file_s3_transfer_config()
        content_type = ''
        if suffix == '.csv':
            content_type = 'text/csv'
        elif suffix == '.txt':
            content_type = 'text/plain'
        elif suffix == '.json':
            content_type = 'application/json'
        elif suffix == '.nc':
            content_type = 'application/x-netcdf'
        output_url = ObjectStorageManager.upload_file_to_s3(
            tmp_file_path, transfer_config, remote_file_path,
            content_type=content_type
        )
        # save file size output
        self.output_metadata['size'] = os.path.getsize(tmp_file_path)
        return output_url

    def to_netcdf_stream(self):
        """Generate netcdf stream."""
        with (
            tempfile.NamedTemporaryFile(
                suffix=".nc", delete=True, delete_on_close=False)
        ) as tmp_file:
            x = self.xr_dataset.to_netcdf(
                tmp_file.name, format='NETCDF4', engine='h5netcdf',
                compute=False
            )
            execute_dask_compute(x, is_api=True)
            self.output_metadata['size'] = os.path.getsize(tmp_file.name)
            with open(tmp_file.name, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk

    def to_netcdf(self):
        """Generate netcdf file to object storage."""
        with (
            tempfile.NamedTemporaryFile(
                suffix=".nc", delete=True, delete_on_close=False)
        ) as tmp_file:
            x = self.xr_dataset.to_netcdf(
                tmp_file.name, format='NETCDF4', engine='h5netcdf',
                compute=False
            )
            execute_dask_compute(x, is_api=True)

            output_url = self._upload_to_s3(
                tmp_file.name, '.nc'
            )

        return output_url

    def _get_chunk_indices(self, chunks):
        indices = []
        start = 0
        for size in chunks:
            stop = start + size
            indices.append((start, stop))
            start = stop
        return indices

    def _get_dataset_for_csv(
        self, date_chunk_size=None, lat_chunk_size=None,
        lon_chunk_size=None
    ):
        dim_order = [self.date_variable]

        if self.has_time_column:
            dim_order.append('time')

        reordered_cols = [
            attribute.attribute.variable_name for attribute in self.attributes
        ]
        # use date chunk = 1 to order by date
        rechunk = {
            self.date_variable: date_chunk_size or 1
        }
        if 'lat' in self.xr_dataset.dims:
            dim_order.append('lat')
            dim_order.append('lon')
            rechunk['lat'] = lat_chunk_size or 300
            rechunk['lon'] = lon_chunk_size or 300
            if self.has_time_column:
                # slightly reducing chunk size for lat/lon
                rechunk['lat'] = lat_chunk_size or 100
                rechunk['lon'] = lon_chunk_size or 100
                rechunk['time'] = 24
        else:
            reordered_cols.insert(0, 'lon')
            reordered_cols.insert(0, 'lat')
            rechunk[self.date_variable] = date_chunk_size or 300
            if self.has_time_column:
                # slightly reducing chunk size for lat/lon
                rechunk[self.date_variable] = date_chunk_size or 100
                rechunk['time'] = 24

        if 'ensemble' in self.xr_dataset.dims:
            dim_order.append('ensemble')
            rechunk['ensemble'] = 50

        # rechunk dataset
        ds = self.xr_dataset.chunk(rechunk)

        if self.has_time_column:
            time_delta = ds['time'].dt.total_seconds().values
            time_str = [
                f"{int(x // 3600):02}:{int((x % 3600) // 60):02}"
                f":{int(x % 60):02}"
                for x in time_delta
            ]
            ds = ds.assign_coords(
                **{'time': ('time', time_str)}
            )

        return ds, dim_order, reordered_cols

    def _filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter dataframe."""
        if not self.has_time_column:
            return df

        # filter the dataframe based on the start and end datetime
        if 'datetime' in df.columns:
            return df[
                (df['datetime'] >= self.start_datetime) &
                (df['datetime'] <= self.end_datetime)
            ]
        df_reset = df.reset_index()
        df_reset['datetime'] = pd.to_datetime(
            df_reset['date'].astype(str) + ' ' + df_reset['time'].astype(str)
        )
        df_reset = df_reset[
            (df_reset['datetime'] >= self.start_datetime) &
            (df_reset['datetime'] <= self.end_datetime)
        ]
        df_reset = df_reset.drop(columns=['datetime'])

        return df_reset.set_index(['date', 'time'])

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream.

        :param suffix: file extension, defaults to '.csv'
        :type suffix: str, optional
        :param separator: separator, defaults to ','
        :type separator: str, optional
        :yield: bytes of csv file
        :rtype: bytes
        """
        ds, dim_order, reordered_cols = self._get_dataset_for_csv()

        date_indices = self._get_chunk_indices(
            ds.chunksizes[self.date_variable]
        )

        # cannot use dask utils because to_dataframe is not returning
        # delayed object
        with dask.config.set(
            pool=ThreadPoolExecutor(get_num_of_threads(is_api=True))
        ):
            if 'lat' in dim_order:
                lat_indices = self._get_chunk_indices(ds.chunksizes['lat'])
                lon_indices = self._get_chunk_indices(ds.chunksizes['lon'])
                write_headers = True
                # iterate foreach chunk
                for date_start, date_stop in date_indices:
                    for lat_start, lat_stop in lat_indices:
                        for lon_start, lon_stop in lon_indices:
                            slice_dict = {
                                self.date_variable: slice(
                                    date_start, date_stop
                                ),
                                'lat': slice(lat_start, lat_stop),
                                'lon': slice(lon_start, lon_stop)
                            }
                            chunk = ds.isel(**slice_dict)
                            chunk_df = chunk.to_dataframe(dim_order=dim_order)
                            chunk_df = chunk_df[reordered_cols]
                            chunk_df = self._filter_df(chunk_df)

                            if write_headers:
                                _columns = list(chunk_df.columns)
                                # remote lat lon if exists in _columns
                                if 'lat' in _columns:
                                    _columns.remove('lat')
                                if 'lon' in _columns:
                                    _columns.remove('lon')
                                headers = dim_order + _columns
                                yield bytes(
                                    separator.join(headers) + '\n',
                                    'utf-8'
                                )
                                write_headers = False

                            yield chunk_df.to_csv(
                                index=True, header=False, float_format='%g',
                                sep=separator
                            )
            else:
                write_headers = True
                # iterate foreach chunk
                for date_start, date_stop in date_indices:
                    slice_dict = {
                        self.date_variable: slice(date_start, date_stop)
                    }
                    chunk = ds.isel(**slice_dict)
                    chunk_df = chunk.to_dataframe(dim_order=dim_order)
                    chunk_df = chunk_df[reordered_cols]
                    chunk_df = self._filter_df(chunk_df)

                    if write_headers:
                        headers = dim_order + list(chunk_df.columns)
                        yield bytes(
                            separator.join(headers) + '\n',
                            'utf-8'
                        )
                        write_headers = False

                    yield chunk_df.to_csv(
                        index=True, header=False, float_format='%g',
                        sep=separator
                    )

    def to_csv(
        self, suffix='.csv', separator=',',
        date_chunk_size=None, lat_chunk_size=None,
        lon_chunk_size=None
    ):
        """Generate csv file to object storage."""
        ds, dim_order, reordered_cols = self._get_dataset_for_csv(
            date_chunk_size, lat_chunk_size, lon_chunk_size
        )

        date_indices = self._get_chunk_indices(
            ds.chunksizes[self.date_variable]
        )
        write_headers = True
        output_url = None
        with (
            tempfile.NamedTemporaryFile(
                suffix=suffix, delete=True, delete_on_close=False)
        ) as tmp_file:
            if 'lat' in dim_order:
                lat_indices = self._get_chunk_indices(
                    ds.chunksizes['lat']
                )
                lon_indices = self._get_chunk_indices(
                    ds.chunksizes['lon']
                )
                # iterate foreach chunk
                for date_start, date_stop in date_indices:
                    for lat_start, lat_stop in lat_indices:
                        for lon_start, lon_stop in lon_indices:
                            slice_dict = {
                                self.date_variable: slice(
                                    date_start, date_stop
                                ),
                                'lat': slice(lat_start, lat_stop),
                                'lon': slice(lon_start, lon_stop)
                            }
                            chunk = ds.isel(**slice_dict)
                            chunk_df = chunk.to_dataframe(
                                dim_order=dim_order
                            )
                            chunk_df = chunk_df[reordered_cols]
                            chunk_df = self._filter_df(chunk_df)

                            chunk_df.to_csv(
                                tmp_file.name, index=True, mode='a',
                                header=write_headers,
                                float_format='%g', sep=separator
                            )
                            if write_headers:
                                write_headers = False
            else:
                # iterate foreach chunk
                for date_start, date_stop in date_indices:
                    slice_dict = {
                        self.date_variable: slice(
                            date_start, date_stop
                        )
                    }
                    chunk = ds.isel(**slice_dict)
                    chunk_df = chunk.to_dataframe(dim_order=dim_order)
                    chunk_df = chunk_df[reordered_cols]
                    chunk_df = self._filter_df(chunk_df)

                    chunk_df.to_csv(
                        tmp_file.name, index=True, mode='a',
                        header=write_headers,
                        float_format='%g', sep=separator
                    )
                    if write_headers:
                        write_headers = False

            output_url = self._upload_to_s3(
                tmp_file.name, suffix
            )

        return output_url


class BaseDatasetReader:
    """Base class for Dataset Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime,
            output_type=DatasetReaderOutputType.JSON
    ) -> None:
        """Initialize BaseDatasetReader class.

        :param dataset: Dataset for reading
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        :param output_type: Output type
        :type output_type: str
        """
        self.dataset = dataset
        self.attributes = attributes
        self.location_input = location_input
        self.start_date = start_date
        self.end_date = end_date
        self.output_type = output_type

    def add_attribute(self, attribute: DatasetAttribute):
        """Add a new attribuute to be read.

        :param attribute: Dataset Attribute
        :type attribute: DatasetAttribute
        """
        is_existing = [a for a in self.attributes if a.id == attribute.id]
        if len(is_existing) == 0:
            self.attributes.append(attribute)

    def get_attributes_metadata(self) -> dict:
        """Get attributes metadata (unit and desc).

        :return: Dictionary of attribute and its metadata
        :rtype: dict
        """
        results = {}
        for attrib in self.attributes:
            results[attrib.attribute.variable_name] = {
                'units': attrib.attribute.unit.name,
                'longname': attrib.attribute.name
            }
        return results

    def read(self):
        """Read values from dataset."""
        if self.dataset.type.type == CastType.HISTORICAL:
            self.read_historical_data(
                self.start_date,
                self.end_date
            )
        elif self.dataset.type.type == CastType.FORECAST:
            self.read_forecast_data(
                self.start_date,
                self.end_date
            )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        pass

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        pass

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        pass

    def _split_date_range(
            self, start_date: datetime, end_date: datetime,
            now: datetime
    ) -> dict:
        """Split a date range into past and future ranges."""
        if end_date < now:
            # Entire range is in the past
            return {'past': (start_date, end_date), 'future': None}
        elif start_date >= now:
            # Entire range is in the future
            return {'past': None, 'future': (start_date, end_date)}
        else:
            # Split into past and future
            return {
                'past': (start_date, now - timedelta(days=1)),
                'future': (now, end_date)
            }

    @property
    def has_time_column(self) -> bool:
        """Check if the output has time column.

        :return: True if time column should exist
        :rtype: bool
        """
        return (
            self.dataset.time_step != DatasetTimeStep.DAILY if
            self.dataset else False
        )
