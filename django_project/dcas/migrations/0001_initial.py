# Generated by Django 4.2.7 on 2024-12-18 00:58

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('gap', '0042_alter_datasourcefilecache_size'),
    ]

    operations = [
        migrations.CreateModel(
            name='DCASConfig',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
                ('is_default', models.BooleanField(default=False)),
            ],
            options={
                'verbose_name': 'Config',
                'db_table': 'dcas_config',
            },
        ),
        migrations.CreateModel(
            name='DCASConfigCountry',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('config', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='dcas.dcasconfig')),
                ('country', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.country')),
            ],
            options={
                'verbose_name': 'Country Config',
                'db_table': 'dcas_config_country',
            },
        ),
    ]
