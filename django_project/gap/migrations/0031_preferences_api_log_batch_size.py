# Generated by Django 4.2.7 on 2024-10-17 19:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0030_preferences_ingestor_config'),
    ]

    operations = [
        migrations.AddField(
            model_name='preferences',
            name='api_log_batch_size',
            field=models.IntegerField(default=500, help_text='Number of API Request logs to be saved in a batch.'),
        ),
    ]