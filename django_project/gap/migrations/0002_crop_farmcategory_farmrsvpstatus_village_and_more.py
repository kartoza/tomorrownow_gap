# Generated by Django 4.2.7 on 2024-08-07 09:55

import django.contrib.gis.db.models.fields
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Crop',
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
        migrations.CreateModel(
            name='FarmCategory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'verbose_name_plural': 'Farm categories',
            },
        ),
        migrations.CreateModel(
            name='FarmRSVPStatus',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'verbose_name': 'Farm RSVP status',
                'verbose_name_plural': 'Farm RSVP statuses',
            },
        ),
        migrations.CreateModel(
            name='Village',
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
        migrations.AlterModelOptions(
            name='attribute',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='dataset',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='datasettype',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='observationtype',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='provider',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='unit',
            options={'ordering': ['name']},
        ),
        migrations.AlterField(
            model_name='ingestorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm')], default='Tahmo', max_length=512),
        ),
        migrations.CreateModel(
            name='Farm',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('unique_id', models.CharField(max_length=255, unique=True)),
                ('geometry', django.contrib.gis.db.models.fields.PointField(srid=4326)),
                ('category', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farmcategory')),
                ('crop', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.crop')),
                ('rsvp_status', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farmrsvpstatus')),
                ('village', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='gap.village')),
            ],
        ),
    ]