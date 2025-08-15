# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Command to find grid.
"""

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import models
from django.contrib.gis.db.models.functions import Centroid
import fiona
import fiona.model
import json
from django.contrib.gis.geos import GEOSGeometry

from gap.utils.dms import (
    decimal_to_dms, dms_string_to_point
)
from gap.models import Grid
from gap.utils.geometry import ST_X, ST_Y


class Command(BaseCommand):
    """Command to find a Grid model instance by its unique id."""

    help = 'Finds a Grid model instance by its unique id'

    def add_arguments(self, parser):
        """Add command line arguments."""
        parser.add_argument(
            'grid_id',
            type=str,
            help=(
                'The unique id of the Grid. Use "all" to extract all grids '
                'or "farmers" to read GeoJSON file of farmers.'
            )
        )

    def extract_all_grids(self):
        """Extract all grids from the database."""
        grids = Grid.objects.select_related(
            'country'
        ).annotate(
            centroid=Centroid('geometry')
        ).annotate(
            lat=ST_Y('centroid'),
            lon=ST_X('centroid'),
            country_name=models.F('country__name')
        ).values(
            'id',
            'unique_id',
            'lat',
            'lon',
            'country_name'
        ).all()
        # convert to dataframe
        df = pd.DataFrame(list(grids))
        # save to excel file
        df.to_excel(
            '/home/web/project/scripts/output/grids.xlsx',
            index=False
        )
        self.stdout.write(
            self.style.SUCCESS('All grids extracted and saved to grids.xlsx')
        )

    def read_geojson_farmers(self):
        """Read GeoJSON file of farmers."""
        file_path = '/home/web/project/scripts/input/map_isda_kenya.geojson'
        total_farmers = 0
        total_farmers_with_grid = 0
        rows = []
        with fiona.open(file_path, 'r') as src:
            for feature in src:
                total_farmers += 1
                geom_str = json.dumps(
                    feature['geometry'], cls=fiona.model.ObjectEncoder
                )
                geom = GEOSGeometry(geom_str)
                # find centroid of the geometry
                geom = geom.centroid
                # print id from properties
                farmer_id = feature['properties'].get('id', 'Unknown ID')
                self.stdout.write(
                    self.style.SUCCESS(f'Farmer ID: {farmer_id}')
                )

                # convert point to DMS
                lat_dms, lon_dms = decimal_to_dms(
                    geom.y, geom.x
                )
                dms_str = f"{lat_dms} {lon_dms}"
                self.stdout.write(
                    self.style.SUCCESS(f'Farmer DMS: {dms_str}')
                )
                self.stdout.write(
                    self.style.SUCCESS(f'Farmer geometry: {geom}')
                )

                # Find grid for the farmer
                grid = Grid.get_grids_by_point(geom).first()
                if grid:
                    total_farmers_with_grid += 1
                    self.stdout.write(
                        self.style.SUCCESS(f'Farmer Grid: {grid.unique_id}')
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR('No grid found for this farmer.')
                    )

                # Append row for DataFrame
                rows.append({
                    'Farm ID': farmer_id,
                    'dms': dms_str,
                })

                # convert point to DMS string
                # dms_point = dms_string_to_point(dms_str)
                # self.stdout.write(
                #     self.style.SUCCESS(f'Farmer DMS Point: {dms_point}')
                # )
            self.stdout.write(
                self.style.SUCCESS(f'Total Farmers: {total_farmers}')
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f'Total Farmers with Grid: {total_farmers_with_grid}'
                ) if total_farmers_with_grid == total_farmers
                else self.style.WARNING(
                    f'Total Farmers with Grid: {total_farmers_with_grid} '
                    f'out of {total_farmers}'
                )
            )
        # save dataframe to excel
        df = pd.DataFrame(rows)
        df.to_excel(
            '/home/web/project/scripts/output/isda_farmers.xlsx',
            index=False
        )

    def handle(self, *args, **options):
        """Handle the command."""
        grid_id = options['grid_id']
        if grid_id == 'all':
            self.extract_all_grids()
            return
        elif grid_id == 'farmers':
            self.stdout.write(
                self.style.SUCCESS('Reading GeoJSON file of farmers...')
            )
            self.read_geojson_farmers()
            return

        try:
            grid = Grid.objects.get(unique_id=grid_id)
            self.stdout.write(self.style.SUCCESS(f'Grid found: {grid}'))
            # print the centroid latitude and longitude
            self.stdout.write(
                f'Centroid Latitude: {grid.geometry.centroid.y}'
            )
            self.stdout.write(
                f'Centroid Longitude: {grid.geometry.centroid.x}'
            )

            # Grid Stats
            count = Grid.objects.count()
            self.stdout.write(f'Total Grids: {count}')
            # grid stats for each country
            country_stats = Grid.objects.values(
                'country__name'
            ).annotate(
                count=models.Count('id')
            ).order_by('country__name')
            for stat in country_stats:
                self.stdout.write(
                    f"Country: {stat['country__name']}, Grids: {stat['count']}"
                )
            self.stdout.write(
                self.style.SUCCESS('Grid stats retrieved successfully.')
            )
        except Grid.DoesNotExist:
            raise CommandError(f'Grid with id {grid_id} does not exist.')
