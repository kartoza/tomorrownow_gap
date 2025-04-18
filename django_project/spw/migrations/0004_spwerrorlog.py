# Generated by Django 4.2.7 on 2025-04-08 14:45

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0052_farmshorttermforecast_farm_forecast_date_idx_and_more'),
        ('spw', '0003_alter_rmodeloutput_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='SPWErrorLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('grid_unique_id', models.CharField(blank=True, max_length=100, null=True)),
                ('generated_date', models.DateField()),
                ('error', models.TextField()),
                ('created_on', models.DateTimeField(auto_now_add=True)),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='spw_error_log', to='gap.farm')),
                ('farm_group', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='gap.farmgroup')),
            ],
        ),
    ]
