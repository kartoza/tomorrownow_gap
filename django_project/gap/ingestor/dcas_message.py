# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor for DCAS Message Config
"""

import pandas as pd
import io

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException,
    AdditionalConfigNotFoundException
)
from gap.models import IngestorSession
from dcas.models import DCASConfig, DCASMessagePriority
from message.models import MessageTemplate, MessageApplication


class Keys:
    """Keys for the data."""

    CODE = 'code'
    MESSAGE_EN = 'message_english'
    MESSAGE_SW = 'message_kiswahili'
    PRIORITY = 'message_priority'

    @staticmethod
    def check_columns(df) -> bool:
        """Check if all columns exist in dataframe.

        :param df: dataframe from csv
        :type df: pd.DataFrame
        :raises FileIsNotCorrectException: When column is missing
        """
        keys = [
            Keys.CODE, Keys.MESSAGE_EN, Keys.MESSAGE_SW,
            Keys.PRIORITY
        ]

        missing = []
        for key in keys:
            if key not in df.columns:
                missing.append(key)

        if missing:
            raise FileIsNotCorrectException(
                f'Column(s) missing: {",".join(missing)}'
            )


class DCASMessageIngestor(BaseIngestor):
    """Ingestor for DCAS Farmer Registry data."""

    def __init__(self, session: IngestorSession, working_dir='/tmp'):
        """Initialize the ingestor with session and working directory."""
        super().__init__(session, working_dir)

    def _delete_existing(self, config: DCASConfig):
        """Delete existing messages for the config."""
        # Delete from MessageTemplate by code that is from DCASMessagePriority
        codes = DCASMessagePriority.objects.filter(
            config=config
        ).values_list('code', flat=True)
        MessageTemplate.objects.filter(code__in=codes).delete()

        DCASMessagePriority.objects.filter(config=config).delete()

    def _run(self):
        config_id = self.get_config('config_id')
        if config_id is None:
            raise AdditionalConfigNotFoundException('config_id')

        config = DCASConfig.objects.get(id=config_id)

        # clear existing messages
        self._delete_existing(config)

        df = pd.read_csv(
            io.BytesIO(self.session.file.read()),
            converters={
                Keys.CODE: str,
            }
        )
        # validate columns
        Keys.check_columns(df)

        try:
            idx = 0
            batch_messages = []
            batch_priorities = []
            batch_size = 500
            for _, row in df.iterrows():
                idx += 1
                code = row[Keys.CODE]
                message_en = row[Keys.MESSAGE_EN]
                message_sw = row[Keys.MESSAGE_SW]
                priority = row[Keys.PRIORITY]

                # Create MessageTemplate
                template = MessageTemplate(
                    code=code,
                    name=code,
                    template=message_en,
                    template_en=message_en,
                    template_sw=message_sw,
                    application=MessageApplication.DCAS,
                    note=f'Priority: {priority}',
                )
                batch_messages.append(template)

                # Create DCASMessagePriority
                priority_obj = DCASMessagePriority(
                    config=config,
                    code=code,
                    priority=priority
                )
                batch_priorities.append(priority_obj)

                # Save in batches
                if idx % batch_size == 0:
                    MessageTemplate.objects.bulk_create(batch_messages)
                    DCASMessagePriority.objects.bulk_create(
                        batch_priorities
                    )
                    batch_messages = []
                    batch_priorities = []

            # Save remaining messages
            if batch_messages:
                MessageTemplate.objects.bulk_create(batch_messages)
                DCASMessagePriority.objects.bulk_create(
                    batch_priorities
                )

        except Exception as ex:
            self._delete_existing(config)
            raise Exception(
                f'Row {idx} : {ex}'
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
