# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

import os
import logging
from datetime import date, datetime, time
from typing import Dict

import pytz
from django.contrib.gis.geos import (
    Point
)
from django.db.models.functions import Lower
from django.db.utils import ProgrammingError
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.exceptions import ValidationError, PermissionDenied
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from urllib.parse import urlparse
from django.conf import settings
from django.utils import timezone

from core.models.object_storage_manager import ObjectStorageManager
from core.models.background_task import TaskStatus
from gap.models import (
    Dataset,
    DatasetObservationType,
    Attribute,
    DatasetAttribute,
    DatasetType,
    Preferences,
)
from gap.providers import get_reader_builder
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    BaseDatasetReader,
    DatasetReaderOutputType
)
from core.utils.date import closest_leap_year
from gap_api.models import (
    DatasetTypeAPIConfig, Location, UserFile,
    Job
)
from gap_api.serializers.common import APIErrorSerializer
from gap_api.utils.helper import ApiTag
from gap_api.mixins import GAPAPILoggingMixin, CounterSlidingWindowThrottle
from permission.models import PermissionType
from gap_api.tasks.job import DataRequestJobExecutor


logger = logging.getLogger(__name__)


def product_type_list():
    """Get product Type list."""
    try:
        return list(
            DatasetType.objects.exclude(
                variable_name='default'
            ).values(
                'variable_name', 'name'
            ).order_by('name')
        )
    except ProgrammingError:
        pass
    except RuntimeError:
        pass
    return []


def dataset_attribute_by_product():
    """Get dict of product and its attribute."""
    results = {}
    try:
        for dataset_type in DatasetType.objects.all():
            results[dataset_type.variable_name] = list(
                DatasetAttribute.objects.select_related(
                    'attribute', 'dataset'
                ).filter(
                    dataset__type=dataset_type
                ).values_list(
                    'attribute__variable_name', flat=True
                ).distinct().order_by('attribute__variable_name')
            )
    except ProgrammingError:
        pass
    except RuntimeError:
        pass
    return results


class MeasurementAPI(GAPAPILoggingMixin, APIView):
    """API class for measurement."""

    date_format = '%Y-%m-%d'
    time_format = '%H:%M:%S'
    permission_classes = [IsAuthenticated]
    throttle_classes = [CounterSlidingWindowThrottle]
    api_parameters = [
        openapi.Parameter(
            'product', openapi.IN_QUERY,
            required=True,
            description='Product type',
            type=openapi.TYPE_STRING,
            enum=[
                'cbam_historical_analysis_bias_adjust',
                'cbam_historical_analysis',
                'arable_ground_observation',
                'disdrometer_ground_observation',
                'tahmo_ground_observation',
                'windborne_radiosonde_observation',
                'cbam_shortterm_forecast',
                'salient_seasonal_forecast'
            ],
            default='cbam_historical_analysis_bias_adjust'
        ),
        openapi.Parameter(
            'attributes',
            openapi.IN_QUERY,
            required=True,
            description='List of attribute name',
            type=openapi.TYPE_ARRAY,
            items=openapi.Items(
                type=openapi.TYPE_STRING,
                enum=[],
                default=''
            )
        ),
        openapi.Parameter(
            'start_date', openapi.IN_QUERY,
            required=True,
            description='Start Date (YYYY-MM-DD or MM-DD for LTN)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'start_time', openapi.IN_QUERY,
            description='Start Time - UTC (HH:MM:SS)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'end_date', openapi.IN_QUERY,
            required=True,
            description='End Date (YYYY-MM-DD or MM-DD for LTN)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'end_time', openapi.IN_QUERY,
            description='End Time - UTC (HH:MM:SS)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'forecast_date', openapi.IN_QUERY,
            description=(
                'Forecast Date for Historical '
                'Salient Downscale, available from 2020 (YYYY-MM-DD)'
            ),
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'output_type', openapi.IN_QUERY,
            required=True,
            description='Returned format',
            type=openapi.TYPE_STRING,
            enum=[
                DatasetReaderOutputType.JSON,
                DatasetReaderOutputType.NETCDF,
                DatasetReaderOutputType.CSV,
                DatasetReaderOutputType.ASCII
            ],
            default=DatasetReaderOutputType.JSON
        ),
        openapi.Parameter(
            'lat', openapi.IN_QUERY,
            description='Latitude',
            type=openapi.TYPE_NUMBER
        ),
        openapi.Parameter(
            'lon', openapi.IN_QUERY,
            description='Longitude',
            type=openapi.TYPE_NUMBER
        ),
        openapi.Parameter(
            'altitudes', openapi.IN_QUERY,
            description='2 value of altitudes: alt_min, alt_max',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'bbox', openapi.IN_QUERY,
            description='Bounding box: long min, lat min, long max, lat max',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'location_name', openapi.IN_QUERY,
            description='User location name that has been uploaded',
            type=openapi.TYPE_STRING
        ),
    ]

    def _get_attribute_filter(self):
        """Get list of attributes in the query parameter.

        :return: attribute list
        :rtype: List[Attribute]
        """
        attributes_str = self.request.GET.get('attributes')
        attributes_str = [a.strip() for a in attributes_str.split(',')]
        return Attribute.objects.filter(variable_name__in=attributes_str)

    def _get_date_filter(self, attr_name, default=None):
        """Get date object from filter (start_date/end_date).

        :param attr_name: request parameter name
        :type attr_name: str
        :return: Date object
        :rtype: date
        """
        date_str = self.request.GET.get(attr_name, None)
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
        time_str = self.request.GET.get(attr_name, None)
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
        lon = self.request.GET.get('lon', None)
        lat = self.request.GET.get('lat', None)
        if lon is not None and lat is not None:
            return DatasetReaderInput.from_point(
                Point(x=float(lon), y=float(lat), srid=4326))

        # (xmin, ymin, xmax, ymax)
        bbox = self.request.GET.get('bbox', None)
        if bbox is not None:
            number_list = [float(a) for a in bbox.split(',')]
            return DatasetReaderInput.from_bbox(number_list)

        # location_name
        location_name = self.request.GET.get('location_name', None)
        if location_name is not None:
            location = Location.objects.filter(
                user=self.request.user,
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
        altitudes_str = self.request.GET.get('altitudes', None)
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
        product = self.request.GET.get('product', None)
        if product is None:
            return ['cbam_historical_analysis']
        return [product.lower()]

    def _get_format_filter(self):
        """Get format filter in the request parameters.

        :return: List of product type lowercase
        :rtype: List[str]
        """
        product = self.request.GET.get(
            'output_type', DatasetReaderOutputType.JSON)
        return product.lower()

    def _get_accel_redirect_response(
        self, presigned_url, file_name, content_type
    ):
        parse_result = urlparse(presigned_url)
        response = Response(
            status=200
        )
        url = presigned_url.replace(
            f"{parse_result.scheme}://{parse_result.netloc}/", ""
        )
        response['X-Accel-Redirect'] = (
            f'/userfiles/{parse_result.scheme}/{parse_result.netloc}/{url}'
        )
        response['Content-Type'] = content_type
        response['Content-Disposition'] = (
            f'attachment; filename="{file_name}"'
        )
        return response

    def _get_accel_redirect_response_job_wait(self, job_id):
        response = Response(
            status=200
        )
        poll_interval = self._preferences.job_executor_config.get(
            'poll_interval', 0.5
        )
        max_wait_time = self._preferences.job_executor_config.get(
            'max_wait_time', 1200
        )
        response['X-Accel-Redirect'] = (
            f'/userjobs/http/django:8001/job/{job_id}/wait?'
            f'poll_interval={poll_interval}&'
            f'max_wait_time={max_wait_time}'
        )
        return response

    def validate_product_type(self, product_filter):
        """Validate user has access to product type.

        :param product_filter: list of product type
        :type product_filter: list
        :raises PermissionDenied: no permission to the product type
        """
        dataset_types = DatasetType.objects.filter(
            variable_name__in=product_filter
        )
        has_perm = True
        for dataset_type in dataset_types:
            has_perm = (
                has_perm and
                self.request.user.has_perm(
                    PermissionType.VIEW_DATASET_TYPE, dataset_type
                )
            )

        if not has_perm:
            raise PermissionDenied({
                'Missing Permission': (
                    f'You don\'t have access to {product_filter}!'
                )
            })

    def validate_output_format(
            self, dataset: Dataset, product_type: str,
            location: DatasetReaderInput, output_format):
        """Validate output format.

        :param dataset: dataset to read
        :type dataset: Dataset
        :param product_type: product type in request
        :type product_type: str
        :param location: location input filter
        :type location: DatasetReaderInput
        :param output_format: output type/format
        :type output_format: str
        :raises ValidationError: invalid request
        """
        if output_format == DatasetReaderOutputType.JSON:
            if location.type != LocationInputType.POINT:
                raise ValidationError({
                    'Invalid Request Parameter': (
                        'Output format json is only available '
                        'for single point query!'
                    )
                })
        elif output_format == DatasetReaderOutputType.NETCDF:
            if (
                dataset.observation_type ==
                DatasetObservationType.UPPER_AIR_OBSERVATION
            ):
                raise ValidationError({
                    'Invalid Request Parameter': (
                        'Output format NETCDF is not available '
                        f'for {product_type}!'
                    )
                })
        elif output_format not in [
            DatasetReaderOutputType.CSV,
            DatasetReaderOutputType.ASCII
        ]:
            raise ValidationError({
                'Invalid Request Parameter': (
                    f'Output format {output_format} is not supported!'
                )
            })

    def validate_dataset_attributes(self, dataset_attributes, output_format):
        """Validate the attributes for the query.

        :param dataset_attributes: dataset attribute list
        :type dataset_attributes: List[DatasetAttribute]
        :param output_format: output type/format
        :type output_format: str
        :raises ValidationError: invalid request
        """
        if len(dataset_attributes) == 0:
            raise ValidationError({
                'Invalid Request Parameter': (
                    'No matching attribute found!'
                )
            })

        if output_format == DatasetReaderOutputType.CSV:
            non_ensemble_count = dataset_attributes.filter(
                ensembles=False
            ).count()
            ensemble_count = dataset_attributes.filter(
                ensembles=True
            ).count()
            if ensemble_count > 0 and non_ensemble_count > 0:
                raise ValidationError({
                    'Invalid Request Parameter': (
                        'CSV: Attribute with ensemble cannot be mixed '
                        'with non-ensemble, please use NetCDF format!'
                    )
                })

    def validate_date_range(self, product_filter, start_dt, end_dt):
        """Validate maximum date range based on product filter.

        :param product_filter: list of product type
        :type product_filter: List[str]
        :param start_dt: start date query
        :type start_dt: datetime
        :param end_dt: end date query
        :type end_dt: datetime
        """
        configs = DatasetTypeAPIConfig.objects.filter(
            type__variable_name__in=product_filter
        )
        diff = end_dt - start_dt

        for config in configs:
            if config.max_daterange == -1:
                continue

            if diff.days + 1 > config.max_daterange:
                raise ValidationError({
                    'Invalid Request Parameter': (
                        f'Maximum date range is {config.max_daterange}'
                    )
                })

    def _get_request_params(self):
        request_params = {}
        for k, v in self.request.GET.items():
            request_params[k] = v
        return request_params

    def _get_user_file(self, location: DatasetReaderInput):
        query_params = self._get_request_params()
        query_params['attributes'] = [
            a.strip() for a in query_params['attributes'].split(',')
        ]
        query_params['geom_type'] = location.type
        query_params['geometry'] = location.geometry.wkt
        return UserFile(
            user=self.request.user,
            name="",
            query_params=query_params
        )

    def prepare_response(self, user_file: UserFile):
        """Prepare response for the user file.

        :param user_file: UserFile object
        :type user_file: UserFile
        :return: Response object
        :rtype: Response
        """
        if user_file is None:
            return Response(
                status=404,
                data={
                    'detail': 'No weather data is found for given queries.'
                }
            )

        # Check if x_accel_redirect is enabled
        if self._preferences.api_use_x_accel_redirect:
            presigned_url = user_file.generate_url()
            file_name = os.path.basename(user_file.name)
            return self._get_accel_redirect_response(
                presigned_url,
                file_name,
                'application/x-netcdf' if file_name.endswith('.nc') else
                'text/csv'
            )

        return ObjectStorageManager.download_file_from_s3(
            remote_file_path=user_file.name
        )

    def get_response_data(self) -> Response:
        """Read data from dataset.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        attributes = self._get_attribute_filter()
        location = self._get_location_filter()
        min_altitudes, max_altitudes = self._get_altitudes_filter()
        start_dt = datetime.combine(
            self._get_date_filter('start_date', date.today()),
            self._get_time_filter('start_time', time.min), tzinfo=pytz.UTC
        )
        end_dt = datetime.combine(
            self._get_date_filter('end_date', date.today()),
            self._get_time_filter('end_time', time.max), tzinfo=pytz.UTC
        )
        output_format = self._get_format_filter()
        if location is None:
            return Response(
                status=400,
                data={
                    'Invalid Request Parameter': (
                        'Missing location input parameter!'
                    )
                }
            )

        dataset_attributes = DatasetAttribute.objects.select_related(
            'dataset', 'attribute'
        ).filter(
            attribute__in=attributes,
            dataset__is_internal_use=False,
            attribute__is_active=True
        )
        product_filter = self._get_product_filter()

        # validate product type access
        self.validate_product_type(product_filter)

        dataset_attributes = dataset_attributes.annotate(
            product_name=Lower('dataset__type__variable_name')
        ).filter(
            product_name__in=product_filter
        ).order_by('dataset__type__variable_name')

        # validate empty dataset_attributes
        self.validate_dataset_attributes(dataset_attributes, output_format)

        # validate output type
        self.validate_output_format(
            dataset_attributes.first().dataset, product_filter, location,
            output_format)

        # validate date range
        self.validate_date_range(product_filter, start_dt, end_dt)

        dataset_dict: Dict[int, BaseDatasetReader] = {}
        for da in dataset_attributes:
            if da.dataset.id in dataset_dict:
                continue
            else:
                try:
                    get_reader_builder(
                        da.dataset, [da], location, start_dt, end_dt,
                        altitudes=(min_altitudes, max_altitudes),
                        use_parquet=self._preferences.api_use_parquet,
                        forecast_date=self._get_date_filter(
                            'forecast_date', None
                        )
                    )
                    dataset_dict[da.dataset.id] = 1
                except TypeError as e:
                    logger.error(
                        f"Error in building dataset reader: {e}",
                        exc_info=True
                    )

        # validate dataset_dict
        if len(dataset_dict) == 0:
            return Response(
                status=400,
                data={
                    'Invalid Request Parameter': (
                        'No matching dataset found!'
                    )
                }
            )

        # Check if the request is async, API will return job ID
        is_async = self.request.GET.get('async', 'false').lower() == 'true'
        use_async_wait = False
        if not is_async:
            # Check if async wait is enabled in preferences
            use_async_wait = self._preferences.job_executor_config.get(
                'use_async_wait', False
            )
        is_execute_immediately = (
            self._preferences.job_executor_config.get(
                'execute_immediately', False
            )
        )

        # Check cache using UserFile
        user_file = self._get_user_file(location)
        cache_exist = user_file.find_in_cache()
        if cache_exist:
            if is_async:
                # prepare job with existing user file
                # Create Job for data request
                job = Job(
                    user=self.request.user,
                    parameters=self._get_request_params(),
                    queue_name=settings.CELERY_DATA_REQUEST_QUEUE,
                    wait_type=0 if is_async else 1,
                    status=TaskStatus.COMPLETED,
                    output_file=cache_exist,
                    finished_at=cache_exist.created_on
                )
                job.save()
                # we may need to update user file last accessed time
                cache_exist.created_on = timezone.now()
                cache_exist.save()
                return Response(
                    status=200,
                    data={
                        'detail': 'Job is submitted successfully.',
                        'job_id': str(job.uuid)
                    }
                )
            return self.prepare_response(cache_exist)

        # Create Job for data request
        job = Job(
            user=self.request.user,
            parameters=self._get_request_params(),
            queue_name=settings.CELERY_DATA_REQUEST_QUEUE,
            wait_type=0 if is_async or use_async_wait else 1,
        )
        job.save()

        executor = DataRequestJobExecutor(
            job, is_main_executor=is_execute_immediately
        )
        executor.run()

        if is_async:
            return Response(
                status=200,
                data={
                    'detail': 'Job is submitted successfully.',
                    'job_id': str(job.uuid)
                }
            )
        elif use_async_wait:
            # Return job ID for async wait
            return self._get_accel_redirect_response_job_wait(
                str(job.uuid)
            )

        job.refresh_from_db()
        if job.status != TaskStatus.COMPLETED:
            return Response(
                status=500,
                data={
                    'detail': 'Job failed to complete.',
                    'errors': job.errors
                }
            )

        if output_format == DatasetReaderOutputType.JSON:
            if job.output_json is None:
                return Response(
                    status=404,
                    data={
                        'detail': 'No weather data is found for given queries.'
                    }
                )
            response = Response(
                status=200,
                data=job.output_json
            )
        else:
            response = self.prepare_response(job.output_file)

        if response is None:
            return Response(
                status=404,
                data={
                    'detail': 'No weather data is found for given queries.'
                }
            )
        return response

    @swagger_auto_schema(
        operation_id='get-measurement',
        operation_description=(
            "Fetch weather data using either a single point or bounding box "
            "and attribute filters."
        ),
        tags=[ApiTag.Measurement],
        manual_parameters=[
            *api_parameters
        ],
        responses={
            200: openapi.Schema(
                description=(
                    'Weather data'
                ),
                type=openapi.TYPE_OBJECT,
                properties={}
            ),
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """Fetch weather data by a single point or bounding box."""
        self._preferences = Preferences.load()

        return self.get_response_data()


class JobStatusAPI(APIView):
    """API class for job status."""

    permission_classes = [IsAuthenticated]
    throttle_classes = [CounterSlidingWindowThrottle]
    api_parameters = [
        openapi.Parameter(
            'job_id', openapi.IN_PATH,
            required=True,
            description='Job ID',
            type=openapi.TYPE_STRING
        )
    ]

    @swagger_auto_schema(
        operation_id='get-job-status',
        operation_description=(
            "Fetch the status of a job by its ID."
        ),
        tags=[ApiTag.Measurement],
        manual_parameters=[
            *api_parameters
        ],
        responses={
            200: openapi.Schema(
                description=(
                    'Job status information'
                ),
                type=openapi.TYPE_OBJECT,
                properties={
                    'job_id': openapi.Schema(
                        type=openapi.TYPE_STRING,
                        description='The ID of the job'
                    ),
                    'status': openapi.Schema(
                        type=openapi.TYPE_STRING,
                        description='Current status of the job'
                    ),
                    'errors': openapi.Schema(
                        type=openapi.TYPE_ARRAY,
                        items=openapi.Items(type=openapi.TYPE_STRING),
                        description='List of errors if any'
                    ),
                    'url': openapi.Schema(
                        type=openapi.TYPE_STRING,
                        description=(
                            'URL to access the output file if available'
                        )
                    ),
                    'data': openapi.Schema(
                        type=openapi.TYPE_OBJECT,
                        description=(
                            'JSON Data if the job has completed successfully'
                        )
                    )
                }
            ),
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """Fetch job status by Job ID."""
        job_id = kwargs.get('job_id', None)
        if job_id is None:
            return Response(
                status=400,
                data={
                    'Invalid Request Parameter': 'Job ID is required.'
                }
            )

        try:
            job_query_filter = {
                'uuid': job_id
            }
            if not request.user.is_superuser:
                job_query_filter['user'] = request.user
            job = Job.objects.get(**job_query_filter)
        except Job.DoesNotExist:
            return Response(
                status=404,
                data={
                    'detail': 'Job not found.'
                }
            )

        if job.status not in [TaskStatus.COMPLETED]:
            return Response(
                status=200,
                data={
                    'job_id': str(job.uuid),
                    'status': job.status,
                    'errors': job.errors,
                    'url': None,
                    'data': None
                }
            )

        response_data = {
            'job_id': str(job.uuid),
            'status': job.status,
            'errors': job.errors,
            'url': None,
            'data': None
        }

        if job.output_file:
            response_data['url'] = job.output_file.generate_url()
            if settings.DEBUG:
                response_data['url'] = response_data['url'].replace(
                    "http://minio:9000", "http://localhost:9010"
                )
        else:
            response_data['data'] = job.output_json

        return Response(
            status=200,
            data=response_data
        )


class MeasurementOptionsView(APIView):
    """API class for measurement options."""

    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        """Fetch available products and attributes."""
        # grab all "products" from DatasetType
        products_qs = DatasetType.objects \
            .exclude(variable_name='default') \
            .values('variable_name', 'name') \
            .order_by('name')
        products = list(products_qs)

        # build a map from each variable_name
        # its available attributes
        attributes: dict[str, list[str]] = {}
        for prod in products:
            var = prod['variable_name']
            vals = DatasetAttribute.objects \
                .filter(dataset__type__variable_name=var) \
                .values_list('attribute__variable_name', flat=True) \
                .distinct() \
                .order_by('attribute__variable_name')
            attributes[var] = list(vals)

        return Response({
            'products': products,
            'attributes': attributes,
        })
