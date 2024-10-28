# Generated by Django 4.2.7 on 2024-10-28 06:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prise', '0002_prisedata_alter_prisemessage_options_prisepest_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='prisedata',
            name='data_type',
            field=models.CharField(choices=[('Near Real Time', 'Near Real Time'), ('Climatology', 'Climatology')], default='Near Real Time', max_length=512),
        ),
    ]