# Generated by Django 4.2.7 on 2024-12-15 19:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0041_preferences_api_use_x_accel_redirect_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datasourcefilecache',
            name='size',
            field=models.PositiveBigIntegerField(default=0),
        ),
    ]
