# Generated by Django 4.2.7 on 2024-08-09 03:03

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import gap.models.crop_insight
import uuid


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('gap', '0003_alter_ingestorsession_ingestor_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='FarmShortTermForecast',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('forecast_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the forecast is made')),
            ],
            options={
                'ordering': ['-forecast_date'],
            },
        ),
        migrations.CreateModel(
            name='Pest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'ordering': ['name'],
                'abstract': False,
            },
        ),
        migrations.AlterField(
            model_name='farm',
            name='crop',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='gap.crop'),
        ),
        migrations.CreateModel(
            name='FarmSuitablePlantingWindowSignal',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('generated_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the signal was generated')),
                ('signal', models.CharField(help_text='Signal value of Suitable Planting Window l.', max_length=512)),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm')),
            ],
            options={
                'ordering': ['-generated_date'],
            },
        ),
        migrations.CreateModel(
            name='FarmShortTermForecastData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('value_date', models.DateField(help_text='Date when the value is occurred on forcast')),
                ('value', models.FloatField(help_text='The value of the forecast attribute')),
                ('attribute', models.ForeignKey(help_text='Forecast attribute', on_delete=django.db.models.deletion.CASCADE, to='gap.datasetattribute')),
                ('forecast', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farmshorttermforecast')),
            ],
            options={
                'ordering': ['attribute', '-value_date'],
            },
        ),
        migrations.AddField(
            model_name='farmshorttermforecast',
            name='farm',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm'),
        ),
        migrations.CreateModel(
            name='FarmProbabilisticWeatherForcast',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('forecast_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the forecast is made')),
                ('forecast_period', models.CharField(help_text="Forecast period (e.g., '2 weeks', 'week 2-4', 'week 4-8', 'week 8-12')", max_length=512)),
                ('temperature_10th_percentile', models.FloatField(help_text='10th percentile of temperature forecast')),
                ('temperature_50th_percentile', models.FloatField(help_text='50th percentile (median) of temperature forecast')),
                ('temperature_90th_percentile', models.FloatField(help_text='90th percentile of temperature forecast')),
                ('precipitation_10th_percentile', models.FloatField(help_text='10th percentile of precipitation forecast')),
                ('precipitation_50th_percentile', models.FloatField(help_text='50th percentile (median) of precipitation forecast')),
                ('precipitation_90th_percentile', models.FloatField(help_text='90th percentile of precipitation forecast')),
                ('other_parameters', models.JSONField(blank=True, help_text='JSON object to store additional probabilistic forecast parameters (e.g., humidity, wind speed)', null=True)),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm')),
            ],
            options={
                'ordering': ['-forecast_date'],
            },
        ),
        migrations.CreateModel(
            name='FarmPlantingWindowTable',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('recommendation_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the recommendation was made')),
                ('recommended_date', models.DateField(help_text='Recommended planting date')),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm')),
            ],
            options={
                'ordering': ['-recommendation_date'],
            },
        ),
        migrations.CreateModel(
            name='FarmPestManagement',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('recommendation_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the recommendation was made')),
                ('spray_recommendation', models.CharField(help_text='Recommended pest spray action', max_length=512)),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm')),
            ],
            options={
                'ordering': ['-recommendation_date'],
            },
        ),
        migrations.CreateModel(
            name='FarmCropVariety',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('recommendation_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the recommendation was made')),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm')),
                ('recommended_crop', models.ForeignKey(help_text='Recommended crop variety', on_delete=django.db.models.deletion.CASCADE, to='gap.crop')),
            ],
            options={
                'ordering': ['-recommendation_date'],
            },
        ),
        migrations.CreateModel(
            name='CropInsightRequest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('unique_id', models.UUIDField(default=uuid.uuid4, editable=False)),
                ('requested_date', models.DateField(default=django.utils.timezone.now, help_text='Date when the request is made')),
                ('file', models.FileField(blank=True, null=True, upload_to=gap.models.crop_insight.ingestor_file_path)),
                ('farms', models.ManyToManyField(to='gap.farm')),
                ('requested_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]