# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Verify Grid Command.
"""

from django.core.management.base import BaseCommand
from django.contrib.gis.db.models.functions import Area, Transform
from django.db.models import F

from gap.models import Grid


class Command(BaseCommand):
    """Command to export all grids into csv file."""

    help = 'Export all grids into csv file.'

    def export_grid_to_csv(self):
        """Export grid to csv file."""
        grids = Grid.objects.annotate(
            geom_m=Transform('geometry', 3857),
            area_sqm=Area('geom_m'),
            area_sqkm=F('area_sqm') / 1_000_000
        )
        print(f'Total grids {grids.count()}')
        with open('grids_wkt.csv', 'w') as f:
            f.write("locationId,name,area_sqkm,geometry\n")
            for grid in grids.iterator(chunk_size=200):
                wkt_escaped = (grid.geometry.wkt or '').replace('"', '""')
                f.write(
                    f'{grid.unique_id},{grid.name},{grid.area_sqkm},'
                    f'"{wkt_escaped}"\n'
                )

    def handle(self, *args, **options):
        """Execute command to verify and export grids."""
        grid = Grid.objects.annotate(
            geom_m=Transform('geometry', 3857),
            area_sqm=Area('geom_m'),
            area_sqkm=F('area_sqm') / 1_000_000
        ).first()
        if grid:
            print(f"Grid ID: {grid.id}, Name: {grid.name}")

            # print area
            print(f"Area: {grid.area_sqkm}")

        self.export_grid_to_csv()
