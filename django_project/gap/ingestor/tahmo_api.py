# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo ingestor.
"""

import os
from datetime import datetime, timedelta

import requests
from django.contrib.gis.geos import Point
from django.utils import timezone
from requests.auth import HTTPBasicAuth

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import EnvIsNotSetException
from gap.models import (
    Country, Provider, StationType, IngestorSession, Dataset,
    DatasetType, DatasetAttribute, Station,
    Measurement, DatasetStore
)
from gap.models.preferences import Preferences
from core.utils.date import find_max_min_epoch_dates

PROVIDER = 'Tahmo'
STATION_TYPE = 'Disdrometer'
DATASET_TYPE = 'disdrometer_ground_observation'
DATASET_NAME = 'Tahmo Disdrometer Observational'
TAHMO_API_USERNAME_ENV_NAME = 'TAHMO_API_USERNAME'
TAHMO_API_PASSWORD_ENV_NAME = 'TAHMO_API_PASSWORD'


class TahmoAPI:
    """Tahmo API."""

    def __init__(self):
        """Initialize Tahmo API."""
        self.base_url = Preferences.load().tahmo_api_url
        if not self.base_url:
            raise Exception('Base URL for tahmo is not set.')

        self.username = os.environ.get(TAHMO_API_USERNAME_ENV_NAME, None)
        self.password = os.environ.get(TAHMO_API_PASSWORD_ENV_NAME, None)

        if not self.username:
            raise EnvIsNotSetException(TAHMO_API_USERNAME_ENV_NAME)
        if not self.password:
            raise EnvIsNotSetException(TAHMO_API_PASSWORD_ENV_NAME)

    @property
    def stations(self):
        """Retrieve all stations."""
        response = requests.get(
            f'{self.base_url}/services/assets/v2/stations',
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if response.status_code == 200:
            return response.json()['data']
        raise Exception(
            f'{response.status_code}: {response.text} : {response.url}'
        )

    def measurements(
            self, station: Station, start_date: datetime, end_date: datetime,
            variable: str
    ):
        """Retrieve all measurements."""
        url = (
            f'{self.base_url}/services/measurements/v2/stations/'
            f'{station.code}/measurements/raw'
        )
        response = requests.get(
            url,
            params={
                "start": start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end": end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "variable": variable
            },
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if response.status_code == 200:
            output = []
            try:
                for result in response.json()['results']:
                    for series in result['series']:
                        output += series['values']
            except KeyError:
                pass
            return output
        raise Exception(
            f'{response.status_code}: {response.text} : {response.url}'
        )


class TahmoAPIIngestor(BaseIngestor):
    """Ingestor for arable data."""

    DEFAULT_FORMAT = DatasetStore.PARQUET
    api_key = None

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)

        self.provider = Provider.objects.get(
            name=PROVIDER
        )
        self.station_type = StationType.objects.get(
            name=STATION_TYPE
        )
        self.dataset_type = DatasetType.objects.get(
            variable_name=DATASET_TYPE
        )
        self.dataset = self._init_dataset()

        self.attributes = {}
        for dataset_attr in self.dataset.datasetattribute_set.all():
            self.attributes[dataset_attr.source] = dataset_attr

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name=DATASET_NAME,
            provider__name=PROVIDER
        )

    @staticmethod
    def last_date_time(
            station: Station, dataset_attribute: DatasetAttribute
    ) -> datetime:
        """Last date measurements of station."""
        # TODO: we need to query the parquet to get the last date time
        first_measurement = Measurement.objects.filter(
            station=station,
            dataset_attribute=dataset_attribute
        ).order_by('-date_time').first()
        if first_measurement:
            return first_measurement.date_time
        return timezone.now() - timedelta(days=365)

    def run(self):
        """Run the ingestor."""
        api = TahmoAPI()
        stations = api.stations
        for station in stations:
            """Save station data."""
            # Skip device that does not have location
            try:
                point = Point(
                    x=station['location']['longitude'],
                    y=station['location']['latitude'],
                    srid=4326
                )
            except (KeyError, IndexError):
                continue

            # Get country
            try:
                country = Country.get_countries_by_point(
                    point
                )[0]
            except IndexError:
                country = None

            # Create station
            station, _ = Station.objects.get_or_create(
                code=station['code'],
                provider=self.provider,
                defaults={
                    'name': station['location']['name'],
                    'geometry': point,
                    'country': country,
                    'station_type': self.station_type,
                    'metadata': {
                        'type': station['location']['type'],
                        'created': station['created']
                    }
                }
            )
            # Save measurements
            end_date = timezone.now()
            min_time = None
            max_time = None
            for variable, dataset_attribute in self.attributes.items():
                start_date = TahmoAPIIngestor.last_date_time(
                    station, dataset_attribute
                )
                measurements = api.measurements(
                    station, start_date, end_date, variable
                )
                for measurement in measurements:
                    try:
                        if measurement[4] is not None:
                            epoch = int(
                                datetime.fromisoformat(
                                    measurement[0]
                                ).timestamp()
                            )
                            min_time, max_time = find_max_min_epoch_dates(
                                min_time, max_time, epoch
                            )
                            Measurement.objects.get_or_create(
                                station=station,
                                dataset_attribute=dataset_attribute,
                                date_time=measurement[0],
                                defaults={
                                    'value': measurement[4]
                                }
                            )
                    except KeyError:
                        pass

            # update the ingested max and min dates
            if min_time:
                self.min_ingested_date = datetime.fromtimestamp(
                    min_time, tz=timezone.utc
                )

            if max_time:
                self.max_ingested_date = datetime.fromtimestamp(
                    max_time, tz=timezone.utc
                )
