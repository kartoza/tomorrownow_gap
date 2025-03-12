# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import os
import logging
from django.core.management.base import BaseCommand
import duckdb


from gap.models import (
    CollectorSession, IngestorSession, IngestorType
)
from gap.ingestor.tio_shortterm import TioHistoricalBackfillIngestor, TioShorttermDuckDBIngestor


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to backfill tio."""

    def handle(self, *args, **options):
        """Run backfill tio."""
        collector = CollectorSession.objects.get(id=108)

        ingestor = IngestorSession.objects.create(
            ingestor_type=IngestorType.TOMORROWIO,
            additional_config={
                'datasourcefile_name': 'tio_final_20250311.zarr',
                'datasourcefile_id': 4573,
                'datasourcefile_exists': True
            },
            trigger_task=False
        )
        ingestor.collectors.set([collector])

        runner = TioShorttermDuckDBIngestor(
            ingestor
        )
        runner.run()
