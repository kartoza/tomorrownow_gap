# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm ingestor.
"""

import json
import pandas as pd
from django.contrib.gis.geos import Point
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    IngestorSession, Farm, FarmGroup, Crop, FarmCategory, FarmRSVPStatus,
    Village
)
from gap.models.ingestor import ingestor_file_path
from gap.utils.dms import dms_string_to_point

COLUMN_COUNT = 9
HEADER_IDX = 1


class Keys:
    """Keys for the data."""

    FARM_ID = 'Farm ID'
    PHONE_NUMBER = 'Phone Number'
    VILLAGE_NAME = 'Village Name'
    GEOMETRY = 'Farm Location (Lat/Long)'
    RSVP = 'RSVP status'
    CATEGORY = 'Category'
    CROP = 'Trial Crop'


class FarmIngestor(BaseIngestor):
    """Ingestor for Farm data."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)
        self.farm_group = None
        try:
            self.farm_group = FarmGroup.objects.get(
                id=session.additional_config['farm_group_id']
            )
        except KeyError:
            raise AdditionalConfigNotFoundException('farm_group_id')
        except FarmGroup.DoesNotExist:
            raise Exception('Farm group does not exist')
        self.skip_existing_farm_id = session.additional_config.get(
            'skip_existing_farm_id', False
        )
        self.duplicate_ids = []

    def is_not_empty(self, value):
        """Check data is empty."""
        return value and f'{value}' != 'nan'

    def _run(self):
        """Run the ingestor."""
        df = pd.read_excel(
            self.session.file.read(), sheet_name=0, header=HEADER_IDX,
            converters={
                Keys.FARM_ID: str,
                Keys.PHONE_NUMBER: str
            }
        )
        df.reset_index(drop=True, inplace=True)
        data = df.to_dict(orient='records')

        # Process the farm
        for idx, row in enumerate(data):
            try:
                farm_id = row[Keys.FARM_ID]
                if self.skip_existing_farm_id:
                    if Farm.objects.filter(unique_id=farm_id).exists():
                        self.duplicate_ids.append(farm_id)
                        continue

                phone_number = None
                if self.is_not_empty(row[Keys.PHONE_NUMBER]):
                    phone_number = row[Keys.PHONE_NUMBER]

                try:
                    geometry = dms_string_to_point(row[Keys.GEOMETRY])
                except ValueError:
                    try:
                        coords = [
                            float(coord) for coord in
                            row[Keys.GEOMETRY].split(',')
                        ]
                        geometry = Point(coords[1], coords[0])
                    except ValueError:
                        raise Exception('Invalid latitude, longitude format')

                crop = None
                if self.is_not_empty(row[Keys.CROP]):
                    crop, _ = Crop.objects.get_or_create(name=row[Keys.CROP])
                category = None
                if self.is_not_empty(row[Keys.CATEGORY]):
                    category, _ = FarmCategory.objects.get_or_create(
                        name=row[Keys.CATEGORY]
                    )
                rsvp_status = None
                if self.is_not_empty(row[Keys.RSVP]):
                    rsvp_status, _ = FarmRSVPStatus.objects.get_or_create(
                        name=row[Keys.RSVP]
                    )
                village = None
                if self.is_not_empty(row[Keys.VILLAGE_NAME]):
                    village, _ = Village.objects.get_or_create(
                        name=row[Keys.VILLAGE_NAME]
                    )
                farm, _ = Farm.objects.update_or_create(
                    unique_id=farm_id,
                    defaults={
                        'geometry': geometry,
                        'crop': crop,
                        'category': category,
                        'rsvp_status': rsvp_status,
                        'village': village,
                        'phone_number': phone_number
                    }
                )
                self.farm_group.farms.add(farm)
            except KeyError as e:
                raise FileIsNotCorrectException(
                    f'Row {idx + HEADER_IDX + 2} does not have {e}'
                )
            except Exception as e:
                raise Exception(
                    f'Row {idx + HEADER_IDX + 2} : {e}'
                )

    def run(self):
        """Run the ingestor."""
        if not self.session.file:
            raise FileNotFoundException()

        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            raise Exception(e)
        finally:
            if self.duplicate_ids:
                # store it in the minio
                file_path = ingestor_file_path(
                    None,
                    f'farm_duplicate_session_{self.session.id}.json'
                )
                content = ContentFile(
                    json.dumps(self.duplicate_ids, separators=(',', ':')))
                default_storage.save(file_path, content)
                # store the path in the additional_config
                self.session.additional_config['duplicate_ids_path'] = (
                    file_path
                )
                self.session.additional_config['duplicate_ids_count'] = (
                    len(self.duplicate_ids)
                )
