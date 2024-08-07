# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

import uuid

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.db import models
from django.core.files.base import ContentFile
from django.utils import timezone

from core.models.common import Definition
from gap.models import Farm
from gap.models.measurement import DatasetAttribute

User = get_user_model()


def ingestor_file_path(instance, filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}crop-insight/{filename}'


class Crop(Definition):
    """Model representing crop."""

    pass


class Pest(Definition):
    """Model representing pest."""

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
    attribute = models.ForeignKey(
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
        ordering = ['attribute', '-value_date']


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
        help_text='Signal value of Suitable Planting Window l.'
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


class CropInsightRequestReport:
    """The report model for the Insight Request Report."""

    def __init__(self, farm: Farm):
        """Initialize the report model for the Insight Request Report."""
        self.farm = farm
        self.latitude = farm.geometry.y
        self.longittude = farm.geometry.x

    @property
    def data(self):
        """Return data."""
        output = {
            'latitude': self.latitude,
            'longitude': self.longittude,
            'spw': None
        }
        spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm
        ).first()
        if spw:
            output['spw'] = spw.signal

        short_term_forcast = FarmShortTermForecast.objects.filter(
            farm=self.farm
        ).first()

        if short_term_forcast:
            datas = short_term_forcast.farmshorttermforecastdata_set.order_by(
                'value_date'
            )
            if datas.first():
                first_date = datas.first().value_date
                for data in datas:
                    difference = data.value_date - first_date
                    key = f'Day {difference.days + 1}'
                    if key not in output:
                        output[key] = {}
                    output[key][
                        data.attribute.attribute.variable_name
                    ] = data.value
        return output


class CropInsightRequest(models.Model):
    """Crop insight request."""

    unique_id = models.UUIDField(
        default=uuid.uuid4, editable=False
    )
    requested_by = models.ForeignKey(
        User, on_delete=models.CASCADE
    )
    requested_date = models.DateField(
        default=timezone.now,
        help_text='Date when the request is made'
    )
    farms = models.ManyToManyField(Farm)
    file = models.FileField(
        upload_to=ingestor_file_path,
        null=True, blank=True
    )

    def generate_report(self):
        """Generate reports."""
        output = [
            ['', '', ''],
            ['Latitude', 'Longitude', 'Suitable Planting Window Signal'],
        ]
        for farm in self.farms.all():
            row = [
                farm.geometry.y,
                farm.geometry.x
            ]
            spw = FarmSuitablePlantingWindowSignal.objects.filter(
                farm=farm
            ).first()
            if spw:
                row.append(spw.signal)
            else:
                row.append('')

            first_columns_count = len(row)

            # Forecast
            forecast = FarmShortTermForecast.objects.filter(
                farm=farm
            ).first()

            if forecast:
                datas = forecast.farmshorttermforecastdata_set.order_by(
                    'value_date'
                )

                if datas.first():
                    first_date = datas.first().value_date
                    last_date = datas.last().value_date
                    different = last_date - first_date

                    # Create the header
                    for idx in range(different.days + 1):
                        key = f'Day {idx + 1}'
                        if key not in output[0]:
                            output[0] += [key, '', '']
                            output[1] += [
                                'Temp (min)', 'Temp (max)', 'Precip (daily)'
                            ]
                        row += ['', '', '']

                    # Create data
                    for data in datas:
                        different = data.value_date - first_date
                        curr_idx = different.days * 3  # 3 columns per day
                        curr_idx += first_columns_count
                        var = data.attribute.source
                        if var == 'temperatureMax':
                            curr_idx += 1
                        elif var == 'rainAccumulationSum':
                            curr_idx += 2
                        row[curr_idx] = data.value

            output.append(row)

        # Render csv
        csv_content = ''
        for row in output:
            csv_content += ','.join(map(str, row)) + '\n'
        content_file = ContentFile(csv_content)
        self.file.save(f'{self.unique_id}.csv', content_file)
        self.save()
