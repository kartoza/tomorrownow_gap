from django.core.management.base import BaseCommand, CommandError
from django.db import models
from gap.models import Grid

class Command(BaseCommand):
    help = 'Finds a Grid model instance by its unique id'

    def add_arguments(self, parser):
        parser.add_argument('grid_id', type=str, help='The unique id of the Grid')

    def handle(self, *args, **options):
        grid_id = options['grid_id']
        try:
            grid = Grid.objects.get(unique_id=grid_id)
            self.stdout.write(self.style.SUCCESS(f'Grid found: {grid}'))
            # print the centroid latitude and longitude
            self.stdout.write(f'Centroid Latitude: {grid.geometry.centroid.y}')
            self.stdout.write(f'Centroid Longitude: {grid.geometry.centroid.x}')

            # Grid Stats
            count = Grid.objects.count()
            self.stdout.write(f'Total Grids: {count}')
            # grid stats for each country
            country_stats = Grid.objects.values('country__name').annotate(count=models.Count('id')).order_by('country__name')
            for stat in country_stats:
                self.stdout.write(f"Country: {stat['country__name']}, Grids: {stat['count']}")
            self.stdout.write(self.style.SUCCESS('Grid stats retrieved successfully.'))
        except Grid.DoesNotExist:
            raise CommandError(f'Grid with id {grid_id} does not exist.')