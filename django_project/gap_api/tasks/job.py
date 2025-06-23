# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Tasks for Executing Jobs
"""

import json
import logging
import time
import pytz
from typing import Dict
from datetime import date, datetime, time as time_s
from core.celery import app
from django.utils import timezone
from django.contrib.gis.geos import (
    Point
)
from django.db.models.functions import Lower
from rest_framework.exceptions import ValidationError

from core.utils.json_encoder import CustomJSONEncoder
from core.utils.date import closest_leap_year
from core.models.background_task import TaskStatus
from gap.models import (
    Attribute,
    DatasetAttribute,
    Preferences
)
from gap.providers import get_reader_builder
from gap.utils.reader import (
    DatasetReaderInput,
    DatasetReaderValue,
    BaseDatasetReader,
    DatasetReaderOutputType
)
from gap_api.models import Job, JobType, UserFile, Location


logger = logging.getLogger(__name__)


class BaseJobExecutor:
    """Base class for executing jobs."""

    DEFAULT_WAIT_TIME = 20 * 60  # 20 minutes
    DEFAULT_WAIT_SLEEP = 0.5  # 0.5 seconds

    def __init__(self, job: Job, is_main_executor=False):
        """Initialize the job executor."""
        self.job = job
        self.job_config = Preferences.load().job_executor_config.get(
            job.job_type, {}
        )
        self.wait_time = self.job_config.get(
            'wait_time', self.DEFAULT_WAIT_TIME
        )
        self.wait_sleep = self.job_config.get(
            'wait_sleep', self.DEFAULT_WAIT_SLEEP
        )
        self.is_main_executor = is_main_executor

    def _get_config(self, key, default=None):
        """Get configuration value for the job type.

        :param key: Configuration key to get
        :type key: str
        :param default: Default value if key is not found
        :return: Configuration value
        """
        return self.job_config.get(key, default)

    def _get_param(self, param_name, default=None):
        """Get parameter from the job parameters.

        :param param_name: Name of the parameter to get
        :type param_name: str
        :param default: Default value if parameter is not found
        :return: Parameter value
        """
        return self.job.parameters.get(param_name, default)

    def _pre_run(self):
        """Pre-run setup for the job."""
        # update job status to running
        self.job.status = TaskStatus.RUNNING
        self.job.started_at = timezone.now()
        self.job.finished_at = None
        self.job.errors = None
        self.job.save()

    def _post_run(self):
        """Post-run cleanup for the job."""
        self.job.status = TaskStatus.COMPLETED
        self.job.finished_at = timezone.now()
        self.job.save()

    def _submit_job(self):
        """Submit job to celery task."""
        raise NotImplementedError(
            "Subclasses must implement the _submit_job method."
        )

    def _run(self):
        """Run the job execution logic."""
        raise NotImplementedError("Subclasses must implement this method.")

    def _wait_for_completion(self):
        """Wait for the job to complete."""
        start_time = time.time()
        while time.time() - start_time < self.wait_time:
            self.job.refresh_from_db()
            if self.job.status in [TaskStatus.COMPLETED, TaskStatus.STOPPED]:
                return True
            if self.job.wait_type == 1:
                time.sleep(self.wait_sleep)
            # TODO: wait using asleep
        logger.warning(
            f"Job {self.job.uuid} did not complete within "
            f"the wait time {self.wait_time}."
        )
        return False

    def _handle_run(self):
        """Handle the run method for the job executor."""
        try:
            self._pre_run()
            self._run()
            self._post_run()
        except Exception as e:
            logger.error(
                f"Error during job execution {self.job.uuid}: {e}",
                exc_info=True
            )
            self.job.status = TaskStatus.STOPPED
            self.job.errors = str(e)
            self.job.finished_at = timezone.now()
            self.job.save()
            raise e

    def run(self):
        """Run the job execution."""
        try:
            if self.is_main_executor:
                self._handle_run()
            elif not self.job.is_async:
                self._handle_run()
            else:
                self._submit_job()
                if self.job.wait_type > 0:
                    is_finished = self._wait_for_completion()
                    if not is_finished:
                        raise TimeoutError(
                            f"Job {self.job.uuid} did not complete in time."
                        )
        except Exception as e:
            logger.error(
                f"Error executing job {self.job.uuid}: {e}",
                exc_info=True
            )
            raise e


class DataRequestJobExecutor(BaseJobExecutor):
    """Executor for Data Request jobs."""

    date_format = '%Y-%m-%d'
    time_format = '%H:%M:%S'

    def __init__(self, job: Job, is_main_executor=False):
        """Initialize the job executor."""
        super().__init__(job, is_main_executor)
        self._preferences = Preferences.load()

    def _get_attribute_filter(self):
        """Get list of attributes in the query parameter.

        :return: attribute list
        :rtype: List[Attribute]
        """
        attributes_str = self._get_param('attributes')
        attributes_str = [a.strip() for a in attributes_str.split(',')]
        return Attribute.objects.filter(variable_name__in=attributes_str)

    def _get_date_filter(self, attr_name, default=None):
        """Get date object from filter (start_date/end_date).

        :param attr_name: request parameter name
        :type attr_name: str
        :return: Date object
        :rtype: date
        """
        date_str = self._get_param(attr_name, None)
        if date_str is None:
            return default
        # check if date_str is in LTN format
        if date_str.count('-') == 1:
            today = date.today()
            # for LTN, use leap year
            date_str = f"{closest_leap_year(today.year)}-{date_str}"
        return (
            datetime.strptime(date_str, self.date_format).date()
        )

    def _get_time_filter(self, attr_name, default):
        """Get time object from filter (start_time/end_time).

        :param attr_name: request parameter name
        :type attr_name: str
        :param default: Time.min or Time.max
        :type default: time
        :return: Time object
        :rtype: time
        """
        time_str = self._get_param(attr_name, None)
        return (
            default if time_str is None else
            datetime.strptime(
                time_str, self.time_format
            ).replace(tzinfo=None).time()
        )

    def _get_location_filter(self) -> DatasetReaderInput:
        """Get location from lon and lat in the request parameters.

        :return: Location to be queried
        :rtype: DatasetReaderInput
        """
        lon = self._get_param('lon', None)
        lat = self._get_param('lat', None)
        if lon is not None and lat is not None:
            return DatasetReaderInput.from_point(
                Point(x=float(lon), y=float(lat), srid=4326))

        # (xmin, ymin, xmax, ymax)
        bbox = self._get_param('bbox', None)
        if bbox is not None:
            number_list = [float(a) for a in bbox.split(',')]
            return DatasetReaderInput.from_bbox(number_list)

        # location_name
        location_name = self._get_param('location_name', None)
        if location_name is not None:
            location = Location.objects.filter(
                user=self.job.user,
                name=location_name
            ).first()
            if location:
                return location.to_input()

        return None

    def _get_altitudes_filter(self):
        """Get list of altitudes in the query parameter.

        :return: altitudes list
        :rtype: (int, int)
        """
        altitudes_str = self._get_param('altitudes', None)
        if altitudes_str is None:
            return None, None
        try:
            altitudes = [
                float(altitude) for altitude in altitudes_str.split(',')
            ]
        except ValueError:
            raise ValidationError('altitudes not a float')
        if len(altitudes) != 2:
            raise ValidationError(
                'altitudes needs to be a comma-separated list, '
                'contains 2 number'
            )
        if altitudes[0] > altitudes[1]:
            raise ValidationError(
                'First altitude needs to be greater than second altitude'
            )
        return altitudes[0], altitudes[1]

    def _get_product_filter(self):
        """Get product name filter in the request parameters.

        :return: List of product type lowercase
        :rtype: List[str]
        """
        product = self._get_param('product', None)
        if product is None:
            return ['cbam_historical_analysis']
        return [product.lower()]

    def _get_format_filter(self):
        """Get format filter in the request parameters.

        :return: List of product type lowercase
        :rtype: List[str]
        """
        product = self._get_param(
            'output_type', DatasetReaderOutputType.JSON)
        return product.lower()

    def _get_user_file(self, location: DatasetReaderInput) -> UserFile:
        query_params = self.job.parameters
        query_params['attributes'] = [
            a.strip() for a in query_params['attributes'].split(',')
        ]
        query_params['geom_type'] = location.type
        query_params['geometry'] = location.geometry.wkt
        return UserFile(
            user=self.job.user,
            name="",
            query_params=query_params
        )

    def _read_data(self, reader: BaseDatasetReader) -> DatasetReaderValue:
        reader.read()
        return reader.get_data_values()

    def _submit_job(self):
        """Submit job to celery task."""
        self.job.status = TaskStatus.PENDING
        self.job.submitted_on = timezone.now()
        self.job.started_at = timezone.now()
        self.job.finished_at = None
        self.job.errors = None
        self.job.save()
        task = execute_data_request_job.apply_async(
            args=[str(self.job.uuid)],
            queue=self.job.queue_name or 'default'
        )
        self.job.task_id = task.id
        self.job.save(update_fields=['task_id'])

    def _read_data_as_json(
        self, reader_dict: Dict[int, BaseDatasetReader],
        start_dt: datetime, end_dt: datetime
    ):
        """Read data from given reader.

        :param reader: Dataset Reader
        :type reader: BaseDatasetReader
        :return: data value
        :rtype: DatasetReaderValue
        """
        data = {
            'metadata': {
                'start_date': start_dt.isoformat(timespec='seconds'),
                'end_date': end_dt.isoformat(timespec='seconds'),
                'dataset': []
            },
            'results': []
        }
        for reader in reader_dict.values():
            reader_value = self._read_data(reader)
            if reader_value.is_empty():
                return None
            values = reader_value.to_json()
            if values:
                data['metadata']['dataset'].append({
                    'provider': reader.dataset.provider.name,
                    'attributes': reader.get_attributes_metadata()
                })
                data['results'].append(values)
        return data

    def _read_data_as_netcdf(
        self, reader_dict: Dict[int, BaseDatasetReader],
        user_file: UserFile
    ):
        reader: BaseDatasetReader = list(reader_dict.values())[0]
        reader_value = self._read_data(reader)
        if reader_value.is_empty():
            return None

        file_path = reader_value.to_netcdf()

        # store the user_file
        user_file.name = file_path
        user_file.size = reader_value.output_metadata.get('size', 0)
        user_file.save()

        return user_file

    def _read_data_as_csv(
        self, reader_dict: Dict[int, BaseDatasetReader],
        user_file: UserFile,
        suffix='.csv', separator=',',
    ):
        reader: BaseDatasetReader = list(reader_dict.values())[0]
        reader_value = self._read_data(reader)
        if reader_value.is_empty():
            return None

        file_path = reader_value.to_csv(
            suffix=suffix,
            separator=separator,
            date_chunk_size=self._get_config(
                'date_chunk_size', None
            ),
            lat_chunk_size=self._get_config(
                'lat_chunk_size', None
            ),
            lon_chunk_size=self._get_config(
                'lon_chunk_size', None
            )
        )

        # store the user_file
        user_file.name = file_path
        user_file.size = reader_value.output_metadata.get('size', 0)
        user_file.save()

        return user_file

    def _run(self):
        attributes = self._get_attribute_filter()
        location = self._get_location_filter()
        min_altitudes, max_altitudes = self._get_altitudes_filter()
        start_dt = datetime.combine(
            self._get_date_filter('start_date', date.today()),
            self._get_time_filter('start_time', time_s.min), tzinfo=pytz.UTC
        )
        end_dt = datetime.combine(
            self._get_date_filter('end_date', date.today()),
            self._get_time_filter('end_time', time_s.max), tzinfo=pytz.UTC
        )
        output_format = self._get_format_filter()
        # fetch dataset attributes
        dataset_attributes = DatasetAttribute.objects.select_related(
            'dataset', 'attribute'
        ).filter(
            attribute__in=attributes,
            dataset__is_internal_use=False,
            attribute__is_active=True
        )
        product_filter = self._get_product_filter()
        dataset_attributes = dataset_attributes.annotate(
            product_name=Lower('dataset__type__variable_name')
        ).filter(
            product_name__in=product_filter
        ).order_by('dataset__type__variable_name')

        # prepare dataset readers
        dataset_dict: Dict[int, BaseDatasetReader] = {}
        for da in dataset_attributes:
            if da.dataset.id in dataset_dict:
                dataset_dict[da.dataset.id].add_attribute(da)
            else:
                try:
                    reader = get_reader_builder(
                        da.dataset, [da], location, start_dt, end_dt,
                        altitudes=(min_altitudes, max_altitudes),
                        use_parquet=self._preferences.api_use_parquet,
                        forecast_date=self._get_date_filter(
                            'forecast_date', None
                        )
                    )
                    dataset_dict[da.dataset.id] = reader.build()
                except TypeError as e:
                    logger.error(
                        f"Error in building dataset reader: {e}",
                        exc_info=True
                    )

        # prepare UserFile object
        user_file = self._get_user_file(location)
        if output_format == DatasetReaderOutputType.JSON:
            json_output = self._read_data_as_json(
                dataset_dict, start_dt, end_dt
            )
            # store to job json output
            self.job.output_json = json.loads(
                json.dumps(json_output, cls=CustomJSONEncoder)
            )
            self.job.save(update_fields=['output_json'])
        elif output_format == DatasetReaderOutputType.NETCDF:
            user_file = self._read_data_as_netcdf(
                dataset_dict, user_file
            )
            self.job.set_user_file(user_file)
        elif output_format == DatasetReaderOutputType.CSV:
            user_file = self._read_data_as_csv(
                dataset_dict, user_file,
                suffix='.csv',
                separator=','
            )
            self.job.set_user_file(user_file)
        elif output_format == DatasetReaderOutputType.ASCII:
            user_file = self._read_data_as_csv(
                dataset_dict, user_file,
                suffix='.txt',
                separator='\t'
            )
            self.job.set_user_file(user_file)


@app.task(name='execute_data_request_job')
def execute_data_request_job(job_id):
    """Execute job for Data Request."""
    job = Job.objects.get(uuid=job_id)
    if job.job_type != JobType.DATA_REQUEST:
        raise ValueError("Job type is not Data Request")

    try:
        executor = DataRequestJobExecutor(job, is_main_executor=True)
        executor.run()
    except Exception as e:
        job.status = TaskStatus.STOPPED
        job.errors = str(e)
        job.finished_at = timezone.now()
        job.save()
        raise e
