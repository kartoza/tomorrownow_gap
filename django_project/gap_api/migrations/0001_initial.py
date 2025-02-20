# Generated by Django 4.2.7 on 2024-10-17 16:50

import datetime
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='APIRequestLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('username_persistent', models.CharField(blank=True, max_length=200, null=True)),
                ('requested_at', models.DateTimeField(db_index=True, default=datetime.datetime.now)),
                ('response_ms', models.PositiveIntegerField(default=0)),
                ('path', models.CharField(db_index=True, help_text='url path', max_length=200)),
                ('view', models.CharField(blank=True, db_index=True, help_text='method called by this endpoint', max_length=200, null=True)),
                ('view_method', models.CharField(blank=True, db_index=True, max_length=200, null=True)),
                ('remote_addr', models.GenericIPAddressField(blank=True, null=True)),
                ('host', models.URLField()),
                ('method', models.CharField(max_length=10)),
                ('user_agent', models.CharField(blank=True, max_length=255)),
                ('data', models.TextField(blank=True, null=True)),
                ('response', models.TextField(blank=True, null=True)),
                ('errors', models.TextField(blank=True, null=True)),
                ('status_code', models.PositiveIntegerField(blank=True, db_index=True, null=True)),
                ('query_params', models.JSONField(blank=True, default=dict, null=True)),
                ('user', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='user_api', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'API Request Log',
                'abstract': False,
            },
        ),
    ]
