# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo ingestor.
"""

import csv
import json
import os
import shutil
import uuid
import tempfile
import logging
from datetime import datetime, timezone
from zipfile import ZipFile

from django.contrib.gis.geos import Point

from gap.ingestor.exceptions import FileNotFoundException
from gap.models import (
    Provider, Station, StationType, Country,
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus,
    Measurement, Dataset, DatasetType, DatasetAttribute
)
from gap.ingestor.base import BaseIngestor


logger = logging.getLogger(__name__)


class TahmoVariable:
    """Contains Tahmo Variable."""

    def __init__(self, name, unit=None):
        """Initialize the Tahmo variable."""
        self.name = name
        self.unit = unit
        self.dataset_attr = None


class TahmoIngestor(BaseIngestor):
    """Ingestor for tahmo data."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)

        self.provider = Provider.objects.get(
            name='Tahmo'
        )
        self.station_type = StationType.objects.get(
            name='Ground Observations'
        )
        self.dataset_type = DatasetType.objects.get(
            variable_name='tahmo_ground_observation'
        )
        self.dataset = self._init_dataset()

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Tahmo Ground Observational',
            provider__name='Tahmo'
        )

    def _run(self, dir_path):
        """Run the ingestor."""
        # check if ingestor should clear existing data.
        clear_existing_data = self.get_config('reset_data', False)
        if clear_existing_data:
            qs = Measurement.objects.filter(
                dataset_attribute__dataset=self.dataset,
                station__provider=self.provider
            )
            logger.info(
                f'TahmoIngestor: Clear existing data - {qs.count()} records'
            )
            qs.delete()

        # check if we should only ingest after certain date
        min_date = self.get_config('min_date', None)
        if min_date:
            min_date = datetime.strptime(
                min_date, '%Y-%m-%d'
            ).replace(tzinfo=timezone.utc)

        # Data is coming from CSV.
        # CSV headers:
        # - longitude
        # - latitude
        # - station code
        # - name
        dataset_attrs_by_key = {}

        # INGEST STATIONS
        for (dirpath, dirnames, filenames) in os.walk(dir_path):
            for filename in filenames:
                try:
                    if 'station' in filename.lower():
                        reader = csv.DictReader(
                            open(os.path.join(dirpath, filename), 'r')
                        )
                        rows = list(reader)
                        progress = IngestorSessionProgress.objects.create(
                            session=self.session,
                            filename=filename,
                            row_count=len(rows)  # noqa
                        )
                        for data in rows:
                            try:
                                point = Point(
                                    x=float(data['longitude']),
                                    y=float(data['latitude']),
                                    srid=4326
                                )
                                try:
                                    country = Country.get_countries_by_point(
                                        point
                                    )[0]
                                except IndexError:
                                    country = None
                                Station.objects.get_or_create(
                                    code=data['station code'],
                                    provider=self.provider,
                                    defaults={
                                        'name': data['name'],
                                        'geometry': point,
                                        'country': country,
                                        'station_type': self.station_type,
                                    }
                                )
                            except KeyError as e:
                                raise Exception(
                                    json.dumps({
                                        'filename': filename,
                                        'data': data,
                                        'error': f'{e}'
                                    })
                                )
                        progress.status = IngestorSessionStatus.SUCCESS
                        progress.save()
                except UnicodeDecodeError:
                    continue

        # INGEST MEASUREMENTS
        for (dirpath, dirnames, filenames) in os.walk(dir_path):
            for filename in filenames:
                code = filename.split('_')[0]
                try:
                    if 'station' in filename.lower():
                        continue
                    station = Station.objects.get(
                        code=code, provider=self.provider
                    )
                    reader = csv.DictReader(
                        open(os.path.join(dirpath, filename), 'r')
                    )
                    rows = list(reader)
                    progress = IngestorSessionProgress.objects.create(
                        session=self.session,
                        filename=filename,
                        row_count=len(rows)  # noqa
                    )
                    measurements = []
                    for data in rows:
                        date = data['Timestamp']  # noqa
                        if not date:
                            continue

                        # Check the datetime
                        try:
                            try:
                                date_time = datetime.strptime(
                                    date, '%Y-%m-%d'
                                ).replace(tzinfo=timezone.utc)
                            except ValueError:
                                date_time = datetime.strptime(
                                    date, '%m/%d/%y'
                                ).replace(tzinfo=timezone.utc)

                        except ValueError as e:
                            progress.status = IngestorSessionStatus.FAILED
                            progress.notes = json.dumps({
                                'filename': filename,
                                'error': f'{e}'
                            })
                            progress.save()
                            continue

                        date_time.replace(second=0)

                        # skip if date is lesser than min_date
                        if min_date and min_date.date() > date_time.date():
                            continue

                        # Save all data
                        for key, value in data.items():  # noqa
                            try:
                                # Get the dataset attributes
                                attr = None
                                try:
                                    attr = dataset_attrs_by_key[key]
                                except KeyError:
                                    pass

                                if not attr:
                                    try:
                                        attr = DatasetAttribute.objects.get(
                                            dataset=self.dataset,
                                            source=key
                                        )
                                    except DatasetAttribute.DoesNotExist:
                                        continue

                                dataset_attrs_by_key[key] = attr

                                # Skip empty one
                                if value == '':
                                    continue

                                # Save the measurements
                                measurements.append(
                                    Measurement(
                                        station_id=station.id,
                                        dataset_attribute=attr,
                                        date_time=date_time,
                                        value=float(value)
                                    )
                                )
                            except (KeyError, ValueError) as e:
                                raise Exception(
                                    json.dumps({
                                        'filename': filename,
                                        'data': data,
                                        'error': f'{e}'
                                    })
                                )
                    Measurement.objects.bulk_create(
                        measurements, ignore_conflicts=True
                    )
                    progress.status = IngestorSessionStatus.SUCCESS
                    progress.save()
                except Station.DoesNotExist:
                    pass

    def run(self):
        """Run the ingestor."""
        if not self.session.file:
            raise FileNotFoundException()

        # Extract file
        dir_path = os.path.join(self.working_dir, str(uuid.uuid4()))
        os.makedirs(dir_path, exist_ok=True)
        with self.session.file.open('rb') as zip_file:
            # Create a NamedTemporaryFile to store the downloaded file
            with tempfile.NamedTemporaryFile(
                delete=False, dir=self.working_dir
            ) as tmp_file:
                tmp_file.write(zip_file.read())
                tmp_file_path = tmp_file.name
        with ZipFile(tmp_file_path, 'r') as zip_ref:
            zip_ref.extractall(dir_path)

        # Run the ingestion
        try:
            self._run(dir_path)
        except Exception as e:
            raise Exception(e)
        finally:
            shutil.rmtree(dir_path)
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
