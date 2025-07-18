# Generated by Django 4.2.7 on 2025-07-11 16:52

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0065_gridset'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasourcefile',
            name='deleted_at',
            field=models.DateTimeField(blank=True, help_text='If the file is deleted, this field will be set.', null=True),
        ),
        migrations.CreateModel(
            name='DataSourceFileRententionConfig',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('days_to_keep', models.PositiveIntegerField(default=1, help_text='Number of days to keep the files before deletion.')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.dataset')),
            ],
            options={
                'unique_together': {('dataset',)},
            },
        ),
    ]
