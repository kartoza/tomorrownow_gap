# Generated by Django 4.2.7 on 2024-08-23 07:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0008_rainfallclassification'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasetattribute',
            name='has_ensembles',
            field=models.BooleanField(default=False),
        ),
    ]