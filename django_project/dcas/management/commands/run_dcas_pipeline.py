# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import logging
import datetime
from django.core.management.base import BaseCommand

from gap.models import (
    FarmRegistryGroup
)
from dcas.pipeline import DCASDataPipeline


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to process DCAS Pipeline."""

    def handle(self, *args, **options):
        """Run DCAS Pipeline."""
        dt = datetime.date(2024, 12, 1)
        farm_registry_group = FarmRegistryGroup.objects.get(id=1)

        pipeline = DCASDataPipeline([farm_registry_group.id], dt)

        pipeline.run()
