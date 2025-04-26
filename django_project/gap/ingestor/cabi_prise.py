# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm ingestor.
"""

import pandas as pd

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException
)
from gap.models import IngestorSession
from prise.models.data import (
    PriseData, PriseDataRawInput, PriseDataByPestRawInput
)

HEADER_IDX = 0


class Keys:
    """Keys for the data."""

    NO = 'No'
    FARM_ID = 'farmID'
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
    ALERT_DATE = 'alert_date'
    ALERT_DATA_TYPE = 'alert_data_type'


class CabiPriseIngestor(BaseIngestor):
    """Ingestor for Cabi Prise data."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)

    def _run(self):
        """Run the ingestor."""
        df = pd.read_excel(
            self.session.file.read(), sheet_name=0, header=HEADER_IDX,
            converters={
                Keys.FARM_ID: str,
            }
        )
        df.reset_index(drop=True, inplace=True)
        data = df.to_dict(orient='records')

        # Process the farm
        for idx, row in enumerate(data):
            try:
                raw_data = PriseDataRawInput(
                    farm_unique_id=row[Keys.FARM_ID],
                    generated_at=row[Keys.ALERT_DATE],
                    values=[],
                    data_type=row[Keys.ALERT_DATA_TYPE]
                )

                # Extract pest values
                for key, val in row.items():
                    if key not in [
                        Keys.NO,
                        Keys.FARM_ID,
                        Keys.LATITUDE,
                        Keys.LONGITUDE,
                        Keys.ALERT_DATE,
                        Keys.ALERT_DATA_TYPE,
                    ]:
                        try:
                            if pd.isna(val):
                                continue

                            raw_data.values.append(
                                PriseDataByPestRawInput(
                                    pest_variable_name=key, value=float(val)
                                )
                            )
                        except ValueError:
                            raise Exception(f'{key} not a float.')

                # Ingest the data
                if len(raw_data.values) > 1:
                    PriseData.insert_data(raw_data)
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
