# Generated by Django 4.2.7 on 2024-12-08 15:55

from django.db import migrations, models
import gap.models.preferences


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0040_datasourcefilecache_size'),
    ]

    operations = [
        migrations.AddField(
            model_name='preferences',
            name='api_use_x_accel_redirect',
            field=models.BooleanField(default=True, help_text='When set to True, Django will send X-Accel-Redirect header to the NGINX to offload the download process to NGINX.'),
        ),
        migrations.AddField(
            model_name='preferences',
            name='user_file_uploader_config',
            field=models.JSONField(blank=True, default=gap.models.preferences.user_file_uploader_config_default, help_text='Config for fsspec uploader to s3 for UserFile.', null=True),
        ),
    ]
