# Generated by Django 4.2.7 on 2024-08-27 14:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0011_preferences_crop_plan_config'),
    ]

    operations = [
        migrations.AddField(
            model_name='attribute',
            name='is_active',
            field=models.BooleanField(default=True),
        ),
        migrations.AlterField(
            model_name='datasourcefile',
            name='format',
            field=models.CharField(choices=[('NETCDF', 'NETCDF'), ('ZARR', 'ZARR'), ('ZIP_FILE', 'ZIP_FILE')], max_length=512),
        ),
        migrations.AlterField(
            model_name='ingestorsession',
            name='status',
            field=models.CharField(choices=[('PENDING', 'PENDING'), ('RUNNING', 'RUNNING'), ('SUCCESS', 'SUCCESS'), ('FAILED', 'FAILED'), ('CANCELLED', 'CANCELLED')], default='PENDING', max_length=512),
        ),
        migrations.CreateModel(
            name='CollectorSession',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ingestor_type', models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io')], default='Tahmo', max_length=512)),
                ('status', models.CharField(choices=[('PENDING', 'PENDING'), ('RUNNING', 'RUNNING'), ('SUCCESS', 'SUCCESS'), ('FAILED', 'FAILED'), ('CANCELLED', 'CANCELLED')], default='PENDING', max_length=512)),
                ('notes', models.TextField(blank=True, null=True)),
                ('run_at', models.DateTimeField(auto_now_add=True)),
                ('end_at', models.DateTimeField(blank=True, null=True)),
                ('additional_config', models.JSONField(blank=True, default=dict, null=True)),
                ('is_cancelled', models.BooleanField(default=False)),
                ('dataset_files', models.ManyToManyField(blank=True, to='gap.datasourcefile')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.AddField(
            model_name='ingestorsession',
            name='collectors',
            field=models.ManyToManyField(blank=True, to='gap.collectorsession'),
        ),
    ]
