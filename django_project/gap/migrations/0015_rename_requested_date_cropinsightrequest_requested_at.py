# Generated by Django 4.2.7 on 2024-09-03 06:52

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0014_preferences_documentation_url'),
    ]

    operations = [
        migrations.RenameField(
            model_name='cropinsightrequest',
            old_name='requested_date',
            new_name='requested_at',
        ),
        migrations.AlterField(
            model_name='cropinsightrequest',
            name='requested_at',
            field=models.DateTimeField(default=django.utils.timezone.now, help_text='The time when the request is made'),
        ),
    ]