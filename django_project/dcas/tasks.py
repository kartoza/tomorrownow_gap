# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Tasks
"""

import os
from celery import shared_task
import datetime
import logging
from django.core.files.storage import default_storage

from gap.models import FarmRegistryGroup, FarmRegistry, Preferences
from dcas.pipeline import DCASDataPipeline


logger = logging.getLogger(__name__)


@shared_task(name="run_dcas")
def run_dcas():
    """Task to run dcas pipeline."""
    current_dt = datetime.datetime.now()
    dcas_config = Preferences.load().dcas_config

    if current_dt.weekday() != dcas_config.get('weekday'):
        logger.info(f'DCAS: skipping weekday {current_dt.weekday()}')
        return

    # load latest farm registry group
    farm_registry_group = FarmRegistryGroup.objects.filter(
        is_latest=True
    ).first()
    if farm_registry_group is None:
        logger.warning('DCAS: No latest farm registry group')
        return

    # check total count
    total_count = FarmRegistry.objects.filter(
        group=farm_registry_group
    ).count()
    if total_count == 0:
        logger.warning('DCAS: No farm registry farm registry group')
        return

    # run pipeline
    request_date = current_dt.date()
    if dcas_config.get('override_request_date', None):
        request_date = datetime.date.fromisoformat(
            dcas_config.get('override_request_date')
        )
    pipeline = DCASDataPipeline(
        farm_registry_group, request_date,
        crop_num_partitions=dcas_config.get('farm_npartitions', None),
        grid_crop_num_partitions=dcas_config.get(
            'grid_crop_npartitions', None
        )
    )
    pipeline.run()

    store_csv = dcas_config.get('store_csv', False)
    if (
        store_csv and
        os.path.exists(pipeline.data_output.output_csv_file_path)
    ):
        s3_storage = default_storage
        with open(pipeline.data_output.output_csv_file_path) as content:
            out_path = os.path.join(
                'dcas_csv',
                os.path.basename(pipeline.data_output.output_csv_file_path)
            )
            s3_storage.save(out_path, content)
