# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import os
import logging
from django.core.management.base import BaseCommand
import duckdb



logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to backfill tio."""

    def read_data(self):
        path = os.path.join(
            '/tmp', 'tio_backfill', 'd68ced10-ceb0-47d3-ab76-67404e915e37.duckdb'
        )
        conn = duckdb.connect(path)
        conn.sql(
            "SELECT count(*) FROM (SELECT DISTINCT grid_id FROM weather) as temp;"
        ).show(max_rows=250)

        # conn.sql(
        #     """
        #     select * from weather where grid_id=21064 order by date
        #     """
        # ).show(max_rows=250)

        conn.sql(
            """
            select distinct date from weather order by date
            """
        ).show(max_rows=250)

        check = conn.sql(
            """
            select date, max_temperature, min_temperature, total_rainfall from weather where grid_id=2 AND (date='2024-10-01' or date='2024-10-02')
            """
        ).to_df()
        print(check.shape[0])
        # print(check['solar_radiation'][0])
        print(check)
        for row in check.itertuples(index=False):
            print(getattr(row, 'max_temperature', None))

        conn.close()

    def handle(self, *args, **options):
        """Run backfill tio."""
        self.read_data()
