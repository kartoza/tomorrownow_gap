# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import os
import logging
from datetime import date
from django.core.management.base import BaseCommand
from django.contrib.gis.geos import Point
import numpy as np
import pytz
import duckdb

from gap.models import (
    DataSourceFile,
    Dataset,
    DatasetStore,
    DatasetAttribute,
    Farm,
    CollectorSession
)
from gap.utils.reader import DatasetReaderInput
from gap.providers.tio import TioZarrReader
from gap.ingestor.tio_shortterm import TioHistoricalBackfillCollector


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to backfill tio."""

    def read_data(self):
        path = os.path.join(
            '/tmp', 'tio_backfill', 'd192a141-6ad7-4fc0-af27-5299cd418ba3.duckdb'
        )
        conn = duckdb.connect(path)
        conn.sql(
            "SELECT * FROM weather;"
        ).show(max_rows=250)

    def handle(self, *args, **options):
        """Run backfill tio."""
        session = CollectorSession.objects.get(id=99)
        collector = TioHistoricalBackfillCollector(
            session, date(2024, 10, 1), date(2024, 10, 31),
            num_threads=10
        )
        collector._run()

        # self.read_data()
