# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GEE Functions to pull Nowcast dataset.
"""

import logging
from datetime import datetime, time, timedelta, timezone
import ee


logger = logging.getLogger(__name__)
COUNTRY_ASSET_ID = 'FAO/GAUL/2015/level0'
NOWCAST_ASSET_ID = (
    'projects/global-precipitation-nowcast/assets/metnet_nowcast'
)
GRAPHCAST_ASSET_ID = (
    'projects/gcp-public-data-weathernext/assets/59572747_4_0'
)
GRAPHCAST_BAND_NAMES = [
    "total_precipitation_6hr",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_temperature"
]
NO_DATA_VALUE = -9999
NOWCAST_SCALE_VALUE = 5000  # same resolution as the original dataset
GRAPHCAST_SCALE_VALUE = 27750


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
            scale=NOWCAST_SCALE_VALUE,
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


def get_latest_graphcast_timestamp(date):
    """Get the latest graphcast image for a given date."""
    # start from one hour behind to include prev day images
    start_datetime = datetime.combine(date, time.min) - timedelta(hours=1)
    end_datetime = datetime.combine(date, time.max)

    graphcast_img = ee.ImageCollection(GRAPHCAST_ASSET_ID) \
        .filterDate(start_datetime, end_datetime) \
        .filterBounds(get_countries()) \
        .sort('start_time', False) \
        .first()

    start_time = graphcast_img.get('start_time').getInfo()
    # start_time, eg. 2025-08-30T06:00:00Z
    return start_time


def extract_graphcast_at_timestamp(timestamp, bucket_name, verbose=False):
    """Extract Graphcast image at a specific timestamp."""
    countries_geom = get_countries()

    start_time_str = datetime.fromtimestamp(
        timestamp, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    graphcast_img = ee.ImageCollection(GRAPHCAST_ASSET_ID) \
        .filter(ee.Filter.eq('start_time', start_time_str)) \
        .filterBounds(countries_geom) \
        .sort('forecast_hour', True)

    tasks = []
    img_list = graphcast_img.toList(graphcast_img.size())
    total_count = img_list.length().getInfo()

    if verbose:
        logger.info(
            f"Extracting at timestamp {timestamp} with "
            f"total graphcast images found: {total_count}"
        )
        logger.info(f"Exporting to bucket: {bucket_name}")

    for i in range(total_count):
        img = ee.Image(img_list.get(i))
        # number of hours since start_time
        forecast_hour = img.get('forecast_hour').getInfo()
        img_id = f'{timestamp}_{forecast_hour}'
        img = img.clip(countries_geom)
        img = img.select(GRAPHCAST_BAND_NAMES)
        task = ee.batch.Export.image.toCloudStorage(
            image=img,
            description=f'graphcast_{img_id}',
            bucket=bucket_name,
            fileNamePrefix=f'graphcast_{img_id}',
            scale=GRAPHCAST_SCALE_VALUE,
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
            'file_name': f'graphcast_{img_id}.tif',
            'start_time': None,
            'elapsed_time': None,
            'progress': None,
            'status': None
        })

    return tasks
