# Generated by Django 4.2.7 on 2024-10-25 05:36

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0034_pest_link_pest_scientific_name_pest_taxonomic_rank'),
        ('prise', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='PriseData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ingested_at', models.DateTimeField(auto_now_add=True, help_text='Date and time at which the prise data was ingested.')),
                ('generated_at', models.DateTimeField(help_text='Date and time at which the prise data was generated.')),
                ('data_type', models.CharField(choices=[('Near Real Time', 'Near Real Time')], default='Near Real Time', max_length=512)),
                ('farm', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.farm')),
            ],
            options={
                'verbose_name': 'Data',
                'db_table': 'prise_data',
                'ordering': ('-generated_at',),
                'unique_together': {('farm', 'generated_at', 'data_type')},
            },
        ),
        migrations.AlterModelOptions(
            name='prisemessage',
            options={'ordering': ('pest__name',), 'verbose_name': 'Message'},
        ),
        migrations.CreateModel(
            name='PrisePest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('variable_name', models.CharField(help_text='Pest variable name that being used on CABI PRISE CSV.', max_length=256, unique=True)),
                ('pest', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.pest')),
            ],
            options={
                'verbose_name': 'Pest',
                'db_table': 'prise_pest',
            },
        ),
        migrations.CreateModel(
            name='PriseDataByPest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('value', models.FloatField()),
                ('data', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='prise.prisedata')),
                ('pest', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.pest')),
            ],
            options={
                'verbose_name': 'Data by Pest',
                'db_table': 'prise_data_by_pest',
                'unique_together': {('data', 'pest')},
            },
        ),
    ]