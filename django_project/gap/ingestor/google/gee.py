# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GEE Functions to pull Nowcast dataset.
"""

import logging
from datetime import datetime, time, timedelta
import ee


logger = logging.getLogger(__name__)
COUNTRY_ASSET_ID = 'FAO/GAUL/2015/level0'
NOWCAST_ASSET_ID = (
    'projects/global-precipitation-nowcast/assets/metnet_nowcast'
)
NO_DATA_VALUE = -9999
SCALE_VALUE = 5000  # same resolution as the original dataset


def get_countries():
    """Get list of country names from the Nowcast dataset."""
    countries = ee.FeatureCollection(COUNTRY_ASSET_ID)
    selected_countries = countries.filter(
        ee.Filter.inList('ADM0_NAME', ['Kenya', 'Malawi', 'Zambia'])
    )
    return selected_countries.geometry()


def get_latest_nowcast_timestamp(date):
    """Get the latest Nowcast image for a given date."""
    # start from one hour behind to include prev day images
    start_datetime = datetime.combine(date, time.min) - timedelta(hours=1)
    end_datetime = datetime.combine(date, time.max)

    nowcast_img = ee.ImageCollection(NOWCAST_ASSET_ID) \
        .filterDate(start_datetime, end_datetime) \
        .filterBounds(get_countries()) \
        .sort('forecast_target_time', False) \
        .filter(ee.Filter.eq('forecast_seconds', 0)) \
        .first()

    timestamp = nowcast_img.get('timestamp').getInfo()

    return timestamp


def extract_nowcast_at_timestamp(timestamp, bucket_name, verbose=False):
    """Extract Nowcast image at a specific timestamp."""
    countries_geom = get_countries()

    nowcast_img = ee.ImageCollection(NOWCAST_ASSET_ID) \
        .filter(ee.Filter.eq('timestamp', timestamp)) \
        .filterBounds(countries_geom) \
        .sort('forecast_target_time', False)

    tasks = []
    img_list = nowcast_img.toList(nowcast_img.size())
    total_count = img_list.length().getInfo()

    if verbose:
        logger.info(
            f"Extracting at timestamp {timestamp} with total nowcast images "
            f"found: {total_count}"
        )
        logger.info(f"Exporting to bucket: {bucket_name}")

    for i in range(total_count):
        img = ee.Image(img_list.get(i))
        img_id = img.get('system:index').getInfo()
        img = img.clip(countries_geom)
        task = ee.batch.Export.image.toCloudStorage(
            image=img,
            description=f'nowcast_{img_id}',
            bucket=bucket_name,
            fileNamePrefix=f'nowcast_{img_id}',
            scale=SCALE_VALUE,
            crs='EPSG:4326',
            region=countries_geom,
            maxPixels=1e13,
            formatOptions={
                'cloudOptimized': True,
                'noData': NO_DATA_VALUE
            }
        )
        tasks.append({
            'task': task,
            'timestamp': timestamp,
            'img_id': img_id,
            'file_name': f'nowcast_{img_id}.tif',
            'start_time': None,
            'elapsed_time': None,
            'progress': None,
            'status': None
        })

    return tasks
