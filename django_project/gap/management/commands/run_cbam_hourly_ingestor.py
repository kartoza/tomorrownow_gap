import time
from django.core.management.base import BaseCommand

from gap.models import CollectorSession, IngestorSession, IngestorType


class Command(BaseCommand):
    help = 'Prints Hello World'

    def handle(self, *args, **options):
        start_time = time.time()
        self.stdout.write('Running CBAM Hourly Collector...')
        session = CollectorSession.objects.get(id=176)
        # session.run()

        # self.stdout.write('CBAM Hourly Collector completed successfully.')

        ingestor_session = IngestorSession.objects.create(
            ingestor_type=IngestorType.HOURLY_CBAM,
            trigger_task=False,
            additional_config={
                'datasourcefile_name': 'cbam_hourly_test1',
                'datasourcefile_id': 10606
            }
        )
        ingestor_session.collectors.set([session])
        ingestor_session.run()
        self.stdout.write('CBAM Hourly Ingestor completed successfully.')
        self.stdout.write(f'Ingestor Session ID: {ingestor_session.id}')

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.stdout.write(f'Time taken: {elapsed_time:.2f} seconds')
