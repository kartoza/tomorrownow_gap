# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import logging
import datetime
from django.core.management.base import BaseCommand

from gap.models import (
    FarmRegistryGroup, Farm, FarmRegistry, Crop, CropStageType
)
from dcas.pipeline import DCASDataPipeline


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to process DCAS Pipeline."""

    def handle(self, *args, **options):
        """Run DCAS Pipeline."""
        dt = datetime.date(2025, 3, 4)
        farm_registry_group = FarmRegistryGroup.objects.get(id=4)

        farm1 = Farm.objects.get(unique_id='4857210')
        print(farm1.grid.id)
        print(farm1.grid.unique_id)
        print(farm1.grid.geometry.centroid)
        farm2 = Farm.objects.get(unique_id='5349220')
        print(farm2.grid.id)
        print(farm2.grid.unique_id)
        print(farm2.grid.geometry.centroid)

        # FarmRegistry.objects.create(
        #     group=farm_registry_group,
        #     farm=farm1,
        #     crop=Crop.objects.get(name='Finger Millet'),
        #     crop_stage_type=CropStageType.objects.get(name='Mid'),
        #     planting_date='2024-11-18'
        # )
        # FarmRegistry.objects.create(
        #     group=farm_registry_group,
        #     farm=farm2,
        #     crop=Crop.objects.get(name='Soybean'),
        #     crop_stage_type=CropStageType.objects.get(name='Early'),
        #     planting_date='2024-11-26'
        # )

        pipeline = DCASDataPipeline(
            [farm_registry_group.id],
            dt,
            farm_num_partitions=1,
            grid_crop_num_partitions=1,
            duck_db_num_threads=2
        )

        pipeline.run()
        file_path = pipeline.extract_csv_output()
        print(file_path)
