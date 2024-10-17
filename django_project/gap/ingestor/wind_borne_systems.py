# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: WindBorne Systems ingestor.
"""

import os
from datetime import datetime

import requests
from django.contrib.gis.geos import Point
from django.utils import timezone
from requests.auth import HTTPBasicAuth

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import EnvIsNotSetException
from gap.models import (
    Provider, StationType, IngestorSession, Dataset,
    DatasetType, DatasetTimeStep, DatasetStore, Station, StationHistory,
    Measurement
)

PROVIDER = 'WindBorne Systems'
STATION_TYPE = 'Balloon'
DATASET_TYPE = 'Airborne Observational'
DATASET_NAME = 'WindBorne Balloons Observations'
USERNAME_ENV_NAME = 'WIND_BORNE_SYSTEMS_USERNAME'
PASSWORD_ENV_NAME = 'WIND_BORNE_SYSTEMS_PASSWORD'


class WindBorneSystemsAPI:
    """WindBorneSystems API."""

    base_url = 'https://sensor-data.windbornesystems.com/api/v1'

    def __init__(self):
        """Initialize WindBorneSystems API."""
        self.username = os.environ.get(USERNAME_ENV_NAME, None)
        self.password = os.environ.get(PASSWORD_ENV_NAME, None)

        if not self.username:
            raise EnvIsNotSetException(USERNAME_ENV_NAME)
        if not self.password:
            raise EnvIsNotSetException(PASSWORD_ENV_NAME)

    def measurements(self, mission_id, since=None) -> (list, int, bool):
        """Return measurements, since and has_next_page."""
        params = {
            'include_ids': True,
            'include_mission_name': True,
            'mission_id': mission_id
        }
        if since:
            params['since'] = since

        response = requests.get(
            f'{self.base_url}/observations.json',
            params=params,
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if response.status_code == 200:
            data = response.json()
            return (
                data['observations'], data['next_since'], data['has_next_page']
            )
        raise Exception(
            f'{response.status_code}: {response.text} : {response.url}'
        )

    def missions(self) -> list:
        """Return missions."""
        response = requests.get(
            f'{self.base_url}/missions.json',
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if response.status_code == 200:
            return response.json()['missions']
        raise Exception(
            f'{response.status_code}: {response.text} : {response.url}'
        )


class WindBorneSystemsIngestor(BaseIngestor):
    """Ingestor for WindBorneSystems."""

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
            name=DATASET_TYPE
        )
        self.dataset, _ = Dataset.objects.get_or_create(
            name=DATASET_NAME,
            provider=self.provider,
            type=self.dataset_type,
            time_step=DatasetTimeStep.DAILY,
            store_type=DatasetStore.TABLE
        )

        self.attributes = {}
        for dataset_attr in self.dataset.datasetattribute_set.all():
            self.attributes[dataset_attr.source] = dataset_attr

    def mission_ids(self, api: WindBorneSystemsAPI):
        """Mission ids."""
        mission_ids = list(
            Station.objects.filter(
                provider=self.provider,
                station_type=self.station_type
            ).values_list('code', flat=True)
        )
        # From API
        mission_ids.extend([mission['id'] for mission in api.missions()])
        return list(set(mission_ids))

    def run(self):
        """Run the ingestor."""
        api = WindBorneSystemsAPI()
        additional_config = self.session.additional_config
        global_since = additional_config.get('since', None)

        # Run for every mission id
        missions = self.mission_ids(api)
        for mission_id in missions:
            print(f'Checking mission {mission_id}')

            mission_since_key = f'since-{mission_id}'
            mission_since = additional_config.get(
                mission_since_key, None
            )
            since = mission_since if mission_since else global_since

            has_next_page = True
            while has_next_page:
                observations, since, has_next_page = api.measurements(
                    mission_id, since
                )

                # Process if it has observations
                if len(observations):
                    for observation in observations:
                        # Get date time
                        date_time = datetime.fromtimestamp(
                            observation['timestamp']
                        )
                        date_time = timezone.make_aware(
                            date_time, timezone.get_default_timezone()
                        )

                        # Points
                        point = Point(
                            x=observation['longitude'],
                            y=observation['latitude'],
                            srid=4326
                        )
                        station, _ = Station.objects.update_or_create(
                            provider=self.provider,
                            station_type=self.station_type,
                            code=observation['mission_id'],
                            defaults={
                                'name': observation['mission_name'],
                                'geometry': point,
                                'altitude': observation['altitude'],
                            }
                        )
                        StationHistory.objects.update_or_create(
                            station=station,
                            date_time=date_time,
                            defaults={
                                'geometry': point,
                                'altitude': observation['altitude'],
                            }
                        )

                        # Save the measurements
                        for variable, attribute in self.attributes.items():
                            try:
                                value = observation[variable]
                                if value is not None:
                                    Measurement.objects.update_or_create(
                                        station=station,
                                        dataset_attribute=attribute,
                                        date_time=date_time,
                                        defaults={
                                            'value': observation[variable]
                                        }
                                    )
                            except KeyError:
                                pass

                    # Save last since for mission
                    additional_config[mission_since_key] = since
                    self.session.additional_config = additional_config
                    self.session.save()
