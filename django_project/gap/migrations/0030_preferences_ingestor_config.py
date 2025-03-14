# Generated by Django 4.2.7 on 2024-10-11 11:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0029_datasourcefilecache_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='preferences',
            name='ingestor_config',
            field=models.JSONField(blank=True, default=dict, help_text='Dict of ProviderName and AdditionalConfig; AdditionalConfig will be passed to the Ingestor Session.', null=True),
        ),
    ]
