# coding=utf-8
"""
Tomorrow Now GAP GDD.

.. note:: Models for GDD Config
"""

from django.contrib.gis.db import models
from gap.models.crop_insight import (
    Crop, CropStageType, CropGrowthStage
)
from dcas.models import DCASConfig


class GDDConfig(models.Model):
    """Model to store base and cap values for each crop."""

    crop = models.ForeignKey(Crop, on_delete=models.CASCADE)
    base_temperature = models.FloatField()
    cap_temperature = models.FloatField()
    config = models.ForeignKey(DCASConfig, on_delete=models.CASCADE)

    class Meta:
        """Meta class for GDDConfig."""

        db_table = 'gdd_config'
        verbose_name = 'GDD Config'
        unique_together = ('crop', 'config')


class GDDMatrix(models.Model):
    """Model to store the matrix of crops and their growth stages."""

    crop = models.ForeignKey(Crop, on_delete=models.CASCADE)
    crop_stage_type = models.ForeignKey(
        CropStageType, on_delete=models.CASCADE)
    crop_growth_stage = models.ForeignKey(
        CropGrowthStage, on_delete=models.CASCADE)
    gdd_threshold = models.FloatField()
    config = models.ForeignKey(DCASConfig, on_delete=models.CASCADE)

    class Meta:
        """Meta class for GDDMatrix."""

        db_table = 'gdd_matrix'
        verbose_name = 'GDD Matrix'
        unique_together = (
            'crop',
            'crop_stage_type',
            'config',
            'crop_growth_stage',
            'gdd_threshold'
        )
