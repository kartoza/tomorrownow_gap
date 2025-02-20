# Generated by Django 4.2.7 on 2024-11-14 19:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0037_alter_collectorsession_ingestor_type_and_more'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='measurement',
            index=models.Index(fields=['dataset_attribute', 'date_time'], name='gap_measure_dataset_c4f74a_idx'),
        ),
        migrations.AddIndex(
            model_name='measurement',
            index=models.Index(fields=['station_history', 'dataset_attribute', 'date_time'], name='gap_measure_station_694206_idx'),
        ),
    ]
