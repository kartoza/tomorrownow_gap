import pandas as pd
import datetime
from django.core.management.base import BaseCommand

from gap.models import Preferences
from spw.tamsat.planting_date_api import routine_operations_v2


class Command(BaseCommand):
    """Command to run TAMSAT planting date API routine operations."""
    help = 'Run TAMSAT planting date API routine operations'

    def handle(self, *args, **options):
        """Run the TAMSAT planting date API routine operations."""
        self.stdout.write(
            self.style.SUCCESS(
                'Running TAMSAT planting date API routine operations...'
            )
        )

        preferences = Preferences.load()
        working_dir = '/tmp'
        farm_group_name = 'KALRO'
        file_path = (
            '/home/web/project/scripts/output/'
            f'SPW_FARM_GROUP_{farm_group_name}.xlsx'
        )

        # convert the excel file to csv file
        df = pd.read_excel(file_path)
        csv_file_path = file_path.replace('.xlsx', '.csv')
        df.to_csv(csv_file_path, index=False)

        # date to run
        date = datetime.date(2025, 4, 30)

        _, obsdata_basic = routine_operations_v2(
            date.year, date.month, date.day, csv_file_path,
            preferences.tamsat_url, working_dir,
            farm_group_name.replace(' ', ''),
            user_col='FarmerID', csv_output=False
        )

        # Save the obsdata_basic DataFrame to a CSV file
        obsdata_basic_file_path = (
            f'/home/web/project/scripts/output/SPW_{farm_group_name}_'
            f'{date.isoformat()}.csv'
        )
        obsdata_basic.to_csv(obsdata_basic_file_path, index=False)
