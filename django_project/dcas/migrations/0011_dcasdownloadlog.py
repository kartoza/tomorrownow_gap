# Generated by Django 4.2.7 on 2025-06-16 08:46

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('dcas', '0010_remove_dcaserrorlog_farm_dcaserrorlog_data_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='DCASDownloadLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('requested_at', models.DateTimeField(auto_now_add=True, help_text='Timestamp when the presigned URL was issued.')),
                ('output', models.ForeignKey(help_text='The DCAS CSV that was downloaded.', on_delete=django.db.models.deletion.CASCADE, related_name='downloads', to='dcas.dcasoutput')),
                ('user', models.ForeignKey(help_text='User who initiated the download.', on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'CSV download',
                'verbose_name_plural': 'CSV downloads',
                'db_table': 'dcas_download_log',
                'ordering': ['-requested_at'],
            },
        ),
    ]
