# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm models
"""

from django.contrib.auth import get_user_model
from django.contrib.gis.db import models

from core.group_email_receiver import crop_plan_receiver
from core.models.common import Definition
from gap.models.farm import Farm

User = get_user_model()


class FarmGroup(Definition):
    """Model representing group of farms."""

    farms = models.ManyToManyField(
        Farm, blank=True
    )
    users = models.ManyToManyField(
        User, blank=True
    )
    phone_number = models.CharField(
        null=True,
        blank=True,
        max_length=255
    )

    def email_recipients(self) -> list:
        """Return list of email addresses of farm recipients."""
        return [
            user.email for user in
            crop_plan_receiver().filter(
                id__in=self.users.all().values_list('id', flat=True)
            )
            if user.email
        ]

    def save(self, *args, **kwargs):
        """Save the group."""
        created = not self.pk
        super(FarmGroup, self).save(*args, **kwargs)
        if created:
            self.prepare_fields()

    def prepare_fields(self):
        """Prepare fields."""
        from gap.models.crop_insight import CropPlanData
        from gap.providers.tio import TomorrowIODatasetReader
        TomorrowIODatasetReader.init_provider()

        self.farmgroupcropinsightfield_set.all().delete()
        column_num = 1
        for default_field in CropPlanData.default_fields():
            active = default_field in CropPlanData.default_fields_used()
            FarmGroupCropInsightField.objects.update_or_create(
                farm_group=self,
                field=default_field,
                defaults={
                    'column_number': column_num,
                    'active': active
                }
            )
            column_num += 1

        # Create for forecast
        forecast_day_n = 13
        for idx in range(forecast_day_n):
            for default_field in (
                    CropPlanData.forecast_fields_used() +
                    CropPlanData.forecast_default_fields()
            ):
                field = CropPlanData.forecast_key(idx + 1, default_field)
                label = field.replace(
                    'rainAccumulationSum', 'mm'
                ).replace(
                    'rainAccumulationType', 'Type'
                ).replace(
                    'precipitationProbability', 'Chance'
                )
                active = default_field in CropPlanData.forecast_fields_used()
                FarmGroupCropInsightField.objects.update_or_create(
                    farm_group=self,
                    field=field,
                    defaults={
                        'column_number': column_num,
                        'label': label if label != field else None,
                        'active': active
                    }
                )
                column_num += 1

    @property
    def headers(self):
        """Return headers."""
        return [
            field.name for field in self.fields
        ]

    @property
    def fields(self):
        """Return headers."""
        return self.farmgroupcropinsightfield_set.filter(active=True)


class FarmGroupCropInsightField(models.Model):
    """Model representing the fields on the crop insight file."""

    farm_group = models.ForeignKey(
        FarmGroup, on_delete=models.CASCADE
    )
    field = models.CharField(
        max_length=256
    )
    column_number = models.PositiveSmallIntegerField(
        help_text='Column number on the csv. It is start from 1',
        default=1
    )
    label = models.CharField(
        max_length=256,
        help_text=(
            'Change the field name to a other label. '
            'Keep it empty if does not want to override'
        ),
        blank=True,
        null=True
    )
    active = models.BooleanField(
        default=True,
        help_text='Check if the field is active and included on the csv'
    )

    class Meta:  # noqa: D106
        ordering = ['column_number']

    @property
    def name(self):
        """Return field key."""
        if self.label:
            return self.label
        return self.field
