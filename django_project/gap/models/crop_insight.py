# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""
import os.path
import uuid
from datetime import date, timedelta, datetime, tzinfo
from concurrent.futures import ThreadPoolExecutor

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.db import models
from django.core.files.base import ContentFile
from django.core.mail import EmailMessage
from django.utils import timezone

from core.models.background_task import BackgroundTask, TaskStatus
from core.models.common import Definition
from gap.exceptions import FarmGroupIsNotSetException
from gap.models.farm import Farm
from gap.models.farm_group import FarmGroup
from gap.models.lookup import RainfallClassification
from gap.models.measurement import DatasetAttribute
from gap.models.preferences import Preferences
from gap.models.pest import Pest
from spw.models import SPWOutput

User = get_user_model()


def ingestor_file_path(instance, filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}crop-insight/{filename}'


class Crop(Definition):
    """Model representing crop."""

    pass


class CropStageType(Definition):
    """Model representing crop stage type."""

    alias = models.CharField(
        max_length=50,
        help_text='Name Alias',
        null=True,
        blank=True
    )


class CropGrowthStage(Definition):
    """Model representing crop growth stage."""

    pass


class FarmShortTermForecast(models.Model):
    """Model representing Farm Short-Term Weather Forecast Table."""

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    forecast_date = models.DateField(
        default=timezone.now,
        help_text='Date when the forecast is made'
    )

    class Meta:  # noqa: D106
        ordering = ['-forecast_date']

    def __str__(self):
        return f'{self.farm.__str__()} - {self.forecast_date}'


class FarmShortTermForecastData(models.Model):
    """Model representing Farm Short-Term Weather Forecast Table data."""

    forecast = models.ForeignKey(
        FarmShortTermForecast, on_delete=models.CASCADE
    )
    dataset_attribute = models.ForeignKey(
        DatasetAttribute, on_delete=models.CASCADE,
        help_text='Forecast attribute'
    )
    value_date = models.DateField(
        help_text='Date when the value is occurred on forcast'
    )
    value = models.FloatField(
        help_text='The value of the forecast attribute'
    )

    class Meta:  # noqa: D106
        ordering = ['dataset_attribute', '-value_date']


class FarmProbabilisticWeatherForcast(models.Model):
    """Model representing Farm Probabilistic S2S Weather Forecast Table.

    Attributes:
        forecast_period (str):
            Forecast period (
                e.g., '2 weeks', 'week 2-4', 'week 4-8', 'week 8-12'
            ).
        temperature_10th_percentile (Float):
            10th percentile of temperature forecast
        temperature_50th_percentile (Float):
            50th percentile (median) of temperature forecast
        temperature_90th_percentile (Float):
            90th percentile of temperature forecast
        precipitation_10th_percentile (Float):
            10th percentile of precipitation forecast
        precipitation_50th_percentile (Float):
            50th percentile (median) of precipitation forecast
        precipitation_90th_percentile (Float):
            90th percentile of precipitation forecast
        other_parameters (dict):
            JSON object to store additional probabilistic forecast parameters
            (e.g., humidity, wind speed)
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    forecast_date = models.DateField(
        default=timezone.now,
        help_text='Date when the forecast is made'
    )
    forecast_period = models.CharField(
        max_length=512,
        help_text=(
            "Forecast period "
            "(e.g., '2 weeks', 'week 2-4', 'week 4-8', 'week 8-12')"
        )
    )
    temperature_10th_percentile = models.FloatField(
        help_text='10th percentile of temperature forecast'
    )
    temperature_50th_percentile = models.FloatField(
        help_text='50th percentile (median) of temperature forecast'
    )
    temperature_90th_percentile = models.FloatField(
        help_text='90th percentile of temperature forecast'
    )
    precipitation_10th_percentile = models.FloatField(
        help_text='10th percentile of precipitation forecast'
    )
    precipitation_50th_percentile = models.FloatField(
        help_text='50th percentile (median) of precipitation forecast'
    )
    precipitation_90th_percentile = models.FloatField(
        help_text='90th percentile of precipitation forecast'
    )
    other_parameters = models.JSONField(
        null=True, blank=True,
        help_text=(
            'JSON object to store additional probabilistic forecast '
            'parameters (e.g., humidity, wind speed)'
        )
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.forecast_date}'

    class Meta:  # noqa: D106
        ordering = ['-forecast_date']


class FarmSuitablePlantingWindowSignal(models.Model):
    """Model representing Farm Suitable Planting Window Signal.

    Attributes:
        signal (str): Suitable planting window signal.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    generated_date = models.DateField(
        default=timezone.now,
        help_text='Date when the signal was generated'
    )
    signal = models.CharField(
        max_length=512,
        help_text='GoNoGo signals.'
    )
    too_wet_indicator = models.CharField(
        max_length=512,
        help_text='Too wet indicator.',
        null=True, blank=True
    )
    last_4_days = models.FloatField(
        help_text='The rain accumulationSum for last 4 days.',
        null=True, blank=True
    )
    last_2_days = models.FloatField(
        help_text='The rain accumulationSum for last 2 days.',
        null=True, blank=True
    )
    today_tomorrow = models.FloatField(
        help_text='The rain accumulationSum for today and tomorrow.',
        null=True, blank=True
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.generated_date}'

    class Meta:  # noqa: D106
        ordering = ['-generated_date']


class FarmPlantingWindowTable(models.Model):
    """Model representing Farm Planting Window Table.

    Attributes:
        recommendation_date (Date): Recommended planting date.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    recommendation_date = models.DateField(
        default=timezone.now,
        help_text='Date when the recommendation was made'
    )
    recommended_date = models.DateField(
        help_text='Recommended planting date'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.recommendation_date}'

    class Meta:  # noqa: D106
        ordering = ['-recommendation_date']


class FarmPestManagement(models.Model):
    """Model representing Farm Pest Management.

    Attributes:
        spray_recommendation (str): Recommended pest spray action.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    recommendation_date = models.DateField(
        default=timezone.now,
        help_text='Date when the recommendation was made'
    )
    spray_recommendation = models.CharField(
        max_length=512,
        help_text='Recommended pest spray action'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.recommendation_date}'

    class Meta:  # noqa: D106
        ordering = ['-recommendation_date']


class FarmCropVariety(models.Model):
    """Model representing Farm Crop Variety.

    Attributes:
        recommended_crop (str): Recommended crop variety.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    recommendation_date = models.DateField(
        default=timezone.now,
        help_text='Date when the recommendation was made'
    )
    recommended_crop = models.ForeignKey(
        Crop, on_delete=models.CASCADE,
        help_text='Recommended crop variety'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.recommendation_date}'

    class Meta:  # noqa: D106
        ordering = ['-recommendation_date']


class CropPlanData:
    """The report model for the Insight Request Report."""

    @staticmethod
    def forecast_default_fields():
        """Return shortterm default fields."""
        from gap.providers.tio import tomorrowio_shortterm_forecast_dataset

        dataset = tomorrowio_shortterm_forecast_dataset()
        forecast_fields = list(
            DatasetAttribute.objects.filter(
                dataset=dataset
            ).values_list(
                'source', flat=True
            )
        )
        if 'rainAccumulationSum' in forecast_fields:
            forecast_fields.append('rainAccumulationType')
        return forecast_fields

    @staticmethod
    def default_fields():
        """Return default fields for Farm Plan Data."""
        return [
            'farmID',
            'phoneNumber',
            'latitude',
            'longitude',
            'SPWTopMessage',
            'SPWDescription',
            'TooWet',
            'last_4_days_mm',
            'last_2_days_mm',
            'today_tomorrow_mm'
        ]

    @staticmethod
    def default_fields_used():
        """Return list of default fields that being used by crop insight."""
        return [
            'farmID',
            'phoneNumber',
            'latitude',
            'longitude',
            'SPWTopMessage',
            'SPWDescription',
            'TooWet',
            'last_4_days_mm',
            'last_2_days_mm',
            'today_tomorrow_mm'
        ]

    def __init__(
            self, farm: Farm, generated_date: date, forecast_days: int = 13,
            forecast_fields: list = None
    ):
        """Initialize the report model for the Insight Request Report."""
        self.generated_date = generated_date
        self.farm = farm

        # Update data
        self.farm_id = self.farm.unique_id
        self.phone_number = self.farm.phone_number

        # load farm lat lon
        lat_lon_digits = Preferences.lat_lon_decimal_digits()
        geometry = farm.geometry
        self.latitude = ''
        self.longitude = ''
        if geometry:
            self.latitude = (
                round(geometry.y, lat_lon_digits) if
                lat_lon_digits != -1 else geometry.y
            )
            self.longitude = (
                round(geometry.x, lat_lon_digits) if
                lat_lon_digits != -1 else geometry.x
            )

        # Forecast
        forecast = FarmShortTermForecast.objects.filter(
            farm=self.farm,
            forecast_date=self.generated_date
        ).first()

        # Check if forecast is found
        if not forecast:
            self.forecast = FarmShortTermForecastData.objects.none()
        else:
            self.forecast = forecast.farmshorttermforecastdata_set.order_by(
                'value_date'
            )

        # Make default forecast_fields
        if not forecast_fields:
            forecast_fields = CropPlanData.forecast_default_fields()

        self.forecast = self.forecast.filter(
            dataset_attribute__source__in=forecast_fields
        ).order_by('value_date')

        self.forecast_fields = forecast_fields
        self.forecast_days = forecast_days

    @staticmethod
    def forecast_key(day_n, field):
        """Return key for forecast field."""
        return f'day{day_n}_{field}'

    @staticmethod
    def forecast_fields_used():
        """Return list of forecast fields that being used by crop insight."""
        return [
            'rainAccumulationSum', 'precipitationProbability',
            'rainAccumulationType'
        ]

    @staticmethod
    def prise_message_key(pest: Pest, field_num):
        """Return key for prise message field."""
        return f'prise_{pest.short_name}_{field_num}'

    @property
    def data(self) -> dict:
        """Return the data."""
        from prise.generator import generate_prise_message

        # ---------------------------------------
        # Spw data
        spw_top_message = ''
        spw_description = ''
        too_wet = ''
        last_4_days = ''
        last_2_days = ''
        today_tomorrow = ''
        spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm,
            generated_date=self.generated_date
        ).first()
        if spw:
            try:
                spw_output = SPWOutput.objects.get(identifier=spw.signal)
                spw_top_message = spw_output.plant_now_string
                spw_description = spw_output.description
            except SPWOutput.DoesNotExist:
                spw_top_message = spw.signal
                spw_description = ''

            if spw.too_wet_indicator is not None:
                too_wet = spw.too_wet_indicator
            if spw.last_4_days is not None:
                last_4_days = spw.last_4_days
            if spw.last_2_days is not None:
                last_2_days = spw.last_2_days
            if spw.today_tomorrow is not None:
                today_tomorrow = spw.today_tomorrow

        # ---------------------------------------
        # Check default_fields functions
        default_fields = CropPlanData.default_fields()
        output = {
            default_fields[0]: self.farm.unique_id,
            default_fields[1]: self.farm.phone_number,
            default_fields[2]: self.latitude,
            default_fields[3]: self.longitude,
            default_fields[4]: spw_top_message,
            default_fields[5]: spw_description,
            default_fields[6]: too_wet,
            default_fields[7]: last_4_days,
            default_fields[8]: last_2_days,
            default_fields[9]: today_tomorrow

        }

        # ----------------------------------------
        # Short term forecast data
        for idx in range(self.forecast_days):
            for field in self.forecast_fields:
                output[CropPlanData.forecast_key(idx + 1, field)] = ''

        first_date = None
        if self.forecast.first():
            first_date = self.forecast.first().value_date

        if first_date:
            # Create forecast data
            for data in self.forecast:
                var = data.dataset_attribute.source
                day_n = (data.value_date - first_date).days + 1
                output[CropPlanData.forecast_key(day_n, var)] = data.value

                if (var == 'rainAccumulationSum' and
                        'rainAccumulationType' in self.forecast_fields):
                    # we get the rain type
                    _class = RainfallClassification.classify(data.value)
                    if _class:
                        output[
                            CropPlanData.forecast_key(
                                day_n, 'rainAccumulationType'
                            )
                        ] = _class.name

        # ----------------------------------------
        # Prise message
        for pest in Pest.objects.all():
            prise_messages = (
                generate_prise_message(self.farm, pest, self.generated_date)
            )
            for idx, prise_message in enumerate(prise_messages):
                output[
                    CropPlanData.prise_message_key(pest, idx + 1)
                ] = prise_message

        return output


class CropInsightRequest(models.Model):
    """Crop insight request."""

    unique_id = models.UUIDField(
        default=uuid.uuid4, editable=False
    )
    requested_by = models.ForeignKey(
        User, on_delete=models.CASCADE
    )
    requested_at = models.DateTimeField(
        default=timezone.now,
        help_text='The time when the request is made'
    )
    farm_group = models.ForeignKey(
        FarmGroup, null=True, blank=True, on_delete=models.CASCADE
    )
    file = models.FileField(
        upload_to=ingestor_file_path,
        null=True, blank=True
    )

    task_names = ['generate_insight_report', 'generate_crop_plan']

    @property
    def last_background_task(self) -> BackgroundTask:
        """Return background task."""
        return BackgroundTask.objects.filter(
            context_id=self.id,
            task_name__in=self.task_names
        ).last()

    @property
    def background_tasks(self):
        """Return background task."""
        return BackgroundTask.objects.filter(
            context_id=self.id,
            task_name__in=self.task_names
        )

    @property
    def background_task_running(self):
        """Return background task that is running."""
        return BackgroundTask.running_tasks().filter(
            context_id=self.id,
            task_name__in=self.task_names
        )

    @staticmethod
    def today_reports():
        """Return query of today reports."""
        now = timezone.now()
        return CropInsightRequest.objects.filter(
            requested_at__gte=now.date(),
            requested_at__lte=now.date() + timedelta(days=1),
        )

    @property
    def skip_run(self):
        """Skip run process."""
        background_task_running = self.background_task_running
        last_running_background_task = background_task_running.last()
        last_background_task = self.last_background_task

        # This rule is based on the second task that basically
        # is already running
        # So we need to check of other task is already running

        # If there are already complete task
        # Skip it
        if self.background_tasks.filter(status=TaskStatus.COMPLETED):
            return True

        # If the last running background task is
        # not same with last background task
        # We skip it as the last running one is other task
        if last_running_background_task and (
                last_running_background_task.id != last_background_task.id
        ):
            return True

        # If there are already running task 2,
        # the current task is skipped
        if background_task_running.count() >= 2:
            return True

        now = timezone.now()
        try:
            if self.requested_at.date() != now.date():
                return True
        except AttributeError:
            if self.requested_at != now.date():
                return True
        return False

    def update_note(self, message):
        """Update the notes."""
        if self.last_background_task:
            self.last_background_task.progress_text = message
            self.last_background_task.save()
        self.notes = message
        self.save()

    def run(self):
        """Run the generate report."""
        if self.skip_run:
            return
        # fetch and generate spw using parallel
        self.generate_spw_by_grid()
        # generate the report
        self._generate_report()

    @property
    def time_description(self) -> (tzinfo, datetime):
        """Return time description."""
        east_africa_timezone = Preferences.east_africa_timezone()
        east_africa_time = self.requested_at.astimezone(east_africa_timezone)
        return east_africa_timezone, east_africa_time

    @property
    def title(self) -> str:
        """Return the title of the request."""
        east_africa_timezone, east_africa_time = self.time_description
        group = ''
        if self.farm_group:
            group = f' {self.farm_group} -'
        return (
            f"GAP - Crop Plan Generator Results -{group} "
            f"{east_africa_time.strftime('%A-%d-%m-%Y')} "
            f"({east_africa_timezone})"
        )

    @property
    def filename(self):
        """Return the filename of the request."""
        east_africa_timezone, east_africa_time = self.time_description
        group = ''
        if self.farm_group:
            group = f' {self.farm_group} -'
        return (
            f"{group} {east_africa_time.strftime('%Y-%m-%d')} "
            f'({self.unique_id}).csv'
        )

    def _process_chunk(self, chunk, port):
        from spw.generator.crop_insight import CropInsightFarmGenerator
        farms = self.farm_group.farms.filter(
            grid_id__in=chunk
        )
        print(f'Farms in port {port} count {farms.count()}')
        for farm in farms:
            if farm.pk:
                CropInsightFarmGenerator(farm, port=port).generate_spw()

    def _chunk_list(self, data):
        """Split the list into smaller chunks."""
        chunk_size = max(1, len(data) // self.num_threads)
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def generate_spw_by_grid(self):
        """Generate spw by grid."""
        self.num_threads = 4
        # If farm is empty, put empty farm
        if self.farm_group:
            farms = self.farm_group.farms.all()
        else:
            raise FarmGroupIsNotSetException()

        # get distinct grid from farms
        grids = farms.values('grid').distinct()
        grid_ids = list(grids.values_list('grid', flat=True))
        chunks = list(self._chunk_list(grid_ids))
        ports = [8282, 8283, 8284, 8285]
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = list(
                executor.map(
                    lambda args: self._process_chunk(*args), zip(chunks, ports)
                )
            )
        for result in results:
            print(result)

    def _generate_report(self):
        """Generate reports."""
        from spw.generator.crop_insight import CropInsightFarmGenerator

        # If farm is empty, put empty farm
        if self.farm_group:
            farms = self.farm_group.farms.all()
        else:
            raise FarmGroupIsNotSetException()

        output = [
            self.farm_group.headers
        ]
        fields = self.farm_group.fields

        # Get farms
        for farm in farms:
            # If it has farm id, generate spw
            if farm.pk:
                self.update_note('Generating SPW for farm: {}'.format(farm))
                CropInsightFarmGenerator(farm).generate_spw()

            data = CropPlanData(
                farm, self.requested_at.date(),
                forecast_fields=[
                    'rainAccumulationSum', 'precipitationProbability',
                    'rainAccumulationType'
                ]
            ).data

            # Create header
            row_data = []
            for field in fields:
                try:
                    row_data.append(data[field.field])
                except KeyError:
                    row_data.append('')
            output.append(row_data)

        # Render csv
        self.update_note('Generate CSV')
        csv_content = ''

        # Save to csv
        for row in output:
            csv_content += ','.join(map(str, row)) + '\n'
        content_file = ContentFile(csv_content)
        self.file.save(
            os.path.join(
                f'{self.farm_group.id}',
                self.filename
            ),
            content_file
        )
        self.save()

        # Send email
        email = EmailMessage(
            subject=self.title,
            body='''
Hi everyone,


Please find the attached file for the crop plan generator results.


Best regards
            ''',
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=self.farm_group.email_recipients()
        )
        email.attach(
            self.filename,
            self.file.open('rb').read(),
            'text/csv'
        )
        email.send()
