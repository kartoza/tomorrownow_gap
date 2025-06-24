# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Ingestor.
"""

from typing import Union, List, Tuple
import logging
import datetime
import pytz
import uuid
import fsspec
import xarray as xr
import numpy as np

from django.utils import timezone
from django.core.files.storage import default_storage
from django.db import transaction

from core.models import BackgroundTask, ObjectStorageManager
from gap.models import (
    CollectorSession,
    IngestorSession,
    IngestorSessionStatus,
    IngestorSessionProgress,
    Dataset,
    DatasetStore,
    DataSourceFile,
    DataSourceFileCache
)
from gap.utils.zarr import BaseZarrReader


logger = logging.getLogger(__name__)


class CoordMapping:
    """Mapping coordinate between Grid and Zarr."""

    def __init__(self, value, nearest_idx, nearest_val) -> None:
        """Initialize coordinate mapping class.

        :param value: lat/lon value from Grid
        :type value: float
        :param nearest_idx: nearest index in Zarr
        :type nearest_idx: int
        :param nearest_val: nearest value in Zarr
        :type nearest_val: float
        """
        self.value = value
        self.nearest_idx = nearest_idx
        self.nearest_val = nearest_val


class BaseIngestor:
    """Collector/Ingestor Base class.

    Available config for ingestor:
    datasourcefile_id: Id of existing DataSourceFile
    datasourcefile_exists: Indicates that Zarr exists on s3,
        or create new one
    datasourcefile_name: set the zarr name if creating new zarr
    remove_temp_file: remove temporary from the collector
    use_latest_datasource: Default to True,
        always use the latest DataSourceFile
    """

    DEFAULT_FORMAT = None

    def __init__(
        self,
        session: Union[CollectorSession, IngestorSession],
        working_dir: str
    ):
        """Initialize ingestor/collector."""
        self.session = session
        self.working_dir = working_dir
        self.min_ingested_date = None
        self.max_ingested_date = None
        # s3 variables for storing the product
        self.s3 = None
        self.s3_options = None
        self._init_s3()

    def is_cancelled(self):
        """Check if session is cancelled by user.

        This method will refetch the session object from DB.
        :return: True if session is gracefully cancelled.
        :rtype: bool
        """
        self.session.refresh_from_db()
        return self.session.is_cancelled

    def get_config(self, name: str, default_value = None):
        """Get config from session.

        :param name: config name
        :type name: str
        :param default_value: default value if config does not exist,
            defaults to None
        :type default_value: any, optional
        :return: config value or default_value
        :rtype: any
        """
        if self.session.additional_config is None:
            return default_value
        return self.session.additional_config.get(name, default_value)

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :raises NotImplementedError: should be implemented in child class
        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        raise NotImplementedError(
            'Ingestor/Collector class must implement init_dataset!'
        )

    def _init_datasource(self) -> Tuple[DataSourceFile, bool]:
        if self.DEFAULT_FORMAT is None:
            raise ValueError('DEFAULT_FORMAT for ingestor class is not set!')

        # get data source file
        datasource_file = None
        created = False
        datasourcefile_id = self.get_config('datasourcefile_id')
        if datasourcefile_id:
            datasource_file = DataSourceFile.objects.get(
                id=datasourcefile_id
            )
            created = not self.get_config(
                'datasourcefile_exists', True
            )
        else:
            datasourcefile_name = self.get_config(
                'datasourcefile_name',
                f'{uuid.uuid4()}{DatasetStore.to_ext(self.DEFAULT_FORMAT)}'
            )
            datasource_file, created = (
                DataSourceFile.objects.get_or_create(
                    name=datasourcefile_name,
                    dataset=self._init_dataset(),
                    format=self.DEFAULT_FORMAT,
                    defaults={
                        'created_on': timezone.now(),
                        'start_date_time': timezone.now(),
                        'end_date_time': (
                            timezone.now()
                        )
                    }
                )
            )

        return datasource_file, created

    def _add_progress(self, progress_name, notes=None):
        """Add progress to the session."""
        return IngestorSessionProgress.objects.create(
            session=self.session,
            filename=progress_name,
            row_count=0,
            notes=notes
        )

    def _init_s3(self):
        """Initialize S3 variables for this ingestor."""
        if self.s3 is None:
            self.s3 = ObjectStorageManager.get_s3_env_vars(
                connection_name=self.get_config(
                    's3_connection_name', None
                )
            )
            self.s3_options = {
                'key': self.s3.get('S3_ACCESS_KEY_ID'),
                'secret': self.s3.get('S3_SECRET_ACCESS_KEY'),
                'client_kwargs': ObjectStorageManager.get_s3_client_kwargs(
                    s3=self.s3
                )
            }


class BaseZarrIngestor(BaseIngestor):
    """Base Ingestor class for Zarr product."""

    DEFAULT_FORMAT = DatasetStore.ZARR
    DATE_VARIABLE = 'date'
    variables = []

    def __init__(self, session, working_dir):
        """Initialize base zarr ingestor."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()

        self.metadata = {}
        self.reindex_tolerance = 0.001
        self.existing_dates = None

        # get zarr data source file
        self.datasource_file, self.created = self._init_datasource()

    def _update_zarr_source_file(self, updated_date: datetime.date):
        """Update zarr DataSourceFile start and end datetime.

        :param updated_date: Date that has been processed
        :type updated_date: datetime.date
        """
        if self.created:
            self.datasource_file.start_date_time = datetime.datetime(
                updated_date.year, updated_date.month, updated_date.day,
                0, 0, 0, tzinfo=pytz.UTC
            )
            self.datasource_file.end_date_time = (
                self.datasource_file.start_date_time
            )
        else:
            if self.datasource_file.start_date_time.date() > updated_date:
                self.datasource_file.start_date_time = datetime.datetime(
                    updated_date.year, updated_date.month,
                    updated_date.day,
                    0, 0, 0, tzinfo=pytz.UTC
                )
            if self.datasource_file.end_date_time.date() < updated_date:
                self.datasource_file.end_date_time = datetime.datetime(
                    updated_date.year, updated_date.month,
                    updated_date.day,
                    0, 0, 0, tzinfo=pytz.UTC
                )
        self.datasource_file.save()

    def _remove_temporary_source_file(
            self, source_file: DataSourceFile, file_path: str):
        """Remove temporary file from collector.

        :param source_file: Temporary File
        :type source_file: DataSourceFile
        :param file_path: s3 file path
        :type file_path: str
        """
        try:
            default_storage.delete(file_path)
        except Exception as ex:
            logger.error(
                f'Failed to remove original source_file {file_path}!', ex)
        finally:
            source_file.delete()

    def _open_zarr_dataset(self, drop_variables = []) -> xr.Dataset:
        """Open existing Zarr file.

        :param drop_variables: variables to exclude from reader
        :type drop_variables: list, optional
        :return: xarray dataset
        :rtype: xr.Dataset
        """
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        return xr.open_zarr(
            s3_mapper, consolidated=True, drop_variables=drop_variables)

    def verify(self):
        """Verify the resulting zarr file."""
        self.zarr_ds = self._open_zarr_dataset()
        print(self.zarr_ds)

    def _invalidate_zarr_cache(self):
        """Invalidate existing zarr cache after ingestor is finished."""
        source_caches = DataSourceFileCache.objects.select_for_update().filter(
            source_file=self.datasource_file
        )
        with transaction.atomic():
            for source_cache in source_caches:
                source_cache.expired_on = timezone.now()
                source_cache.save()

    def _find_chunk_slices(
            self, arr_length: int, chunk_size: int) -> List:
        """Create chunk slices for processing Tio data.

        Given arr with length 300 and chunk_size 150,
        this method will return [slice(0, 150), slice(150, 300)].
        :param arr_length: length of array
        :type arr_length: int
        :param chunk_size: chunk size
        :type chunk_size: int
        :return: list of slice
        :rtype: List
        """
        coord_slices = []
        for coord_range in range(0, arr_length, chunk_size):
            max_idx = coord_range + chunk_size
            coord_slices.append(
                slice(
                    coord_range,
                    max_idx if max_idx < arr_length else arr_length
                )
            )
        return coord_slices

    def _is_sorted_and_incremented(self, arr):
        """Check if array is sorted ascending and incremented by 1.

        :param arr: array
        :type arr: List
        :return: True if array is sorted and incremented by 1
        :rtype: bool
        """
        if not arr:
            return False
        if len(arr) == 1:
            return True
        return all(arr[i] + 1 == arr[i + 1] for i in range(len(arr) - 1))

    def _transform_coordinates_array(
        self, coord_arr, coord_type,
        tolerance = None, fix_incremented: bool = False
    ) -> List[CoordMapping]:
        """Find nearest in Zarr for array of lat/lon/date.

        :param coord_arr: array of lat/lon/date
        :type coord_arr: List[float]
        :param coord_type: lat or lon
        :type coord_type: str
        :return: List CoordMapping with nearest val/idx
        :rtype: List[CoordMapping]
        """
        # open existing zarr
        ds = self._open_zarr_dataset()

        tolerance = tolerance or self.reindex_tolerance
        # find nearest coordinate for each item
        prev_coord_idx = None
        results: List[CoordMapping] = []
        for target_coord in coord_arr:
            if coord_type in ['lat', 'lon']:
                nearest_coord = ds[coord_type].sel(
                    **{coord_type: target_coord}, method='nearest',
                    tolerance=tolerance
                ).item()
            else:
                nearest_coord = target_coord

            coord_idx = np.where(ds[coord_type].values == nearest_coord)[0][0]
            if fix_incremented and prev_coord_idx is not None:
                # if previous coordinate is not the same as current,
                # we need to add the missing coordinate
                if coord_idx != prev_coord_idx + 1:
                    # add missing coordinate
                    missing_coord = ds[coord_type].values[
                        prev_coord_idx + 1:coord_idx
                    ]
                    for idx, mc in enumerate(missing_coord):
                        results.append(
                            CoordMapping(mc, prev_coord_idx + 1 + idx, mc)
                        )
            prev_coord_idx = coord_idx
            # append result
            results.append(
                CoordMapping(target_coord, coord_idx, nearest_coord)
            )

        # close dataset
        ds.close()

        return results

    def _is_date_in_zarr(self, date: datetime.date) -> bool:
        """Check whether a date has been added to zarr file.

        :param date: date to check
        :type date: date
        :return: True if date exists in zarr file.
        :rtype: bool
        """
        if self.created:
            return False
        if self.existing_dates is None:
            ds = self._open_zarr_dataset(self.variables)
            self.existing_dates = ds[self.DATE_VARIABLE].values
            ds.close()
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in self.existing_dates


def ingestor_revoked_handler(bg_task: BackgroundTask):
    """Event handler when ingestor task is cancelled by celery.

    :param bg_task: background task
    :type bg_task: BackgroundTask
    """
    # retrieve ingestor session
    session = IngestorSession.objects.filter(
        id=int(bg_task.context_id)
    ).first()
    if session is None:
        return

    # update status as cancelled
    session.status = IngestorSessionStatus.CANCELLED
    session.save(update_fields=['status'])
