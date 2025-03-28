# Generated by Django 4.2.7 on 2024-10-02 07:45

import django.contrib.gis.db.models.fields
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0026_alter_collectorsession_ingestor_type_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='station',
            name='altitude',
            field=models.FloatField(blank=True, help_text='Altitude in meters', null=True),
        ),
        migrations.AlterField(
            model_name='collectorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io'), ('Arable', 'Arable'), ('Grid', 'Grid'), ('Tahmo API', 'Tahmo API'), ('Tio Forecast Collector', 'Tio Forecast Collector'), ('WindBorne Systems API', 'WindBorne Systems API')], default='Tahmo', max_length=512),
        ),
        migrations.AlterField(
            model_name='ingestorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io'), ('Arable', 'Arable'), ('Grid', 'Grid'), ('Tahmo API', 'Tahmo API'), ('Tio Forecast Collector', 'Tio Forecast Collector'), ('WindBorne Systems API', 'WindBorne Systems API')], default='Tahmo', max_length=512),
        ),
        migrations.AlterField(
            model_name='station',
            name='country',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='gap.country'),
        ),
        migrations.CreateModel(
            name='StationHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('geometry', django.contrib.gis.db.models.fields.PointField(srid=4326)),
                ('altitude', models.FloatField(blank=True, help_text='Altitude in meters', null=True)),
                ('date_time', models.DateTimeField()),
                ('station', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.station')),
            ],
            options={
                'ordering': ('station', 'date_time'),
                'unique_together': {('station', 'date_time')},
            },
        ),
    ]
