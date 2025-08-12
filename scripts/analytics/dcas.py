# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Analytics functions.
"""

import pandas as pd
from datetime import timedelta

from config import get_settings
from dataset import DatasetReaderBuilder, DatasetType
from helper.duckdb import get_duckdb_connection


GDD_BASE_CAP = {
    'maize': {
        'base': 10,
        'cap': 35
    },
    'green gram': {
        'base': 10,
        'cap': 35
    },
    'finger millet': {
        'base': 9,
        'cap': 25
    },
    'sorghum': {
        'base': 7,
        'cap': 35
    },
    'cassava': {
        'base': 12,
        'cap': 35
    },
    'potato': {
        'base': 4.5,
        'cap': 20
    },
    'sunflower': {
        'base': 10,
        'cap': 30
    },
    'soybeans': {
        'base': 10,
        'cap': 30
    },
    'cowpea': {
        'base': 10,
        'cap': 30
    },
}


def get_dcas_stats(date):
    """Get DCAS statistics for a specific date.

    Args:
        date (datetime.date): The date for which to fetch DCAS statistics.
    Returns:
        dict: Dictionary containing the count of DCAS signals.
    """
    settings = get_settings()
    conn = get_duckdb_connection()
    query = f"""
        SELECT COUNT(*) AS signal_count
        FROM read_parquet(
            's3://{settings.S3_BUCKET_NAME}/{settings.DCAS_GEOPARQUET_PATH}/iso_a3=*/year={date.year}/month={date.month}/day={date.day}/*.parquet',
            hive_partitioning=True
        )
        WHERE farm_unique_id IS NOT NULL;
    """
    result = conn.execute(query).fetchone()
    conn.close()
    
    print(f'DCAS statistics for {date}: {result[0]} signals found.')

    return {'signal_count': result[0]} if result else {'signal_count': 0}


def read_dcas_geoparquet(date, farmer_ids):
    """Read DCAS GeoParquet data for a specific month and year.

    Args:
        date (datetime.date): The date for which to fetch DCAS data.
        farmer_ids (list): List of farmer IDs to filter the data.
    Returns:
        pd.DataFrame: DataFrame containing DCAS data for the specified date and farmers.
    """
    settings = get_settings()
    conn = get_duckdb_connection()
    query = f"""
        SELECT * FROM read_parquet(
            's3://{settings.S3_BUCKET_NAME}/{settings.DCAS_GEOPARQUET_PATH}/iso_a3=*/year={date.year}/month={date.month}/day={date.day}/*.parquet',
            hive_partitioning=True
        )
        WHERE farm_unique_id IN $1;
    """
    print(f'Reading DCAS data for date: {date} and farmers: {len(farmer_ids)}')
    df = conn.execute(query, (farmer_ids,)).df()

    print(f'Length of DCAS Data: {len(df)}')
    print(f'Columns in DCAS Data: {df.columns.tolist()}')
    dcas_display_columns = [
        'date', 'farm_unique_id', 'crop', 'message',
        'message_2', 'message_3', 'message_4', 'message_5'
    ]
    if 'planting_date' in df.columns:
        dcas_display_columns.append('planting_date')
    elif 'planting_date_epoch' in df.columns:
        # Convert epoch to date
        df['planting_date'] = pd.to_datetime(df['planting_date_epoch'], unit='s').dt.date
        dcas_display_columns.append('planting_date')
    # convert growth_stage_start_date to date
    if 'growth_stage_start_date' in df.columns:
        df['growth_stage_start_date'] = pd.to_datetime(df['growth_stage_start_date'], unit='s').dt.date
    print(df[dcas_display_columns].head())
    if len(df) == 0:
        print('⚠️ No DCAS data found for the specified date and farmers.')
    elif len(df) != len(farmer_ids):
        print(f'⚠️ Warning: Length of DCAS Data ({len(df)}) does not match Farmer List ({len(farmer_ids)})')
    elif len(df) == len(farmer_ids):
        print('✅ Length of DCAS Data matches Farmer List')

    conn.close()
    return df


def read_dcas_error_log_file(input_file):
    """Read the DCAS error log file.

    Returns:
        pd.DataFrame: DataFrame containing the DCAS error log.
    """
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
        print(f'Read {len(df)} rows from DCAS error log file.')

        df['FarmerId'] = df['FarmerId'].astype(str).str.replace(r'\.0$', '', regex=True)
        return df
    except Exception as e:
        print(f'Error reading DCAS error log file: {e}')
        return pd.DataFrame()


def find_crop_base_cap_temp(crop):
    """Find the base and cap temperature for a given crop.

    Args:
        crop (str): The crop name.
    Returns:
        tuple: Base and cap temperature for the crop.
    """
    lower_crop = crop.lower()
    for key, value in GDD_BASE_CAP.items():
        if key in lower_crop:
            return value['base'], value['cap']
    print(f'Warning: No base and cap temperature found for crop: {crop}')
    return None, None

def _do_gdd_calculation(df, base, cap, skip_first_row=True):
    """Perform GDD calculation on the DataFrame."""
    df['max_temperature'] = df['max_temperature'].clip(upper=cap)
    df['min_temperature'] = df['min_temperature'].clip(lower=base)
    df['avg_temperature'] = (df['max_temperature'] + df['min_temperature']) / 2.0
    df['gdd'] = df['avg_temperature'] - base

    # replace the first row with NaN
    # because GDD calculation starts from the day after planting
    if skip_first_row:
        df.loc[df.index[0], 'gdd'] = None

    # add cumulative GDD
    df['gdd_sum'] = df['gdd'].cumsum()

    return df


def calculate_gdd(lat, lon, planting_date, current_date, crop):
    """Calculate Growing Degree Days (GDD) for a given latitude and longitude.

    Args:
        lat (float): Latitude of the location.
        lon (float): Longitude of the location.
        start_date (datetime.date): Start date for GDD calculation.
        end_date (datetime.date): End date for GDD calculation.
    Returns:
        float: Calculated GDD value.
    """
    gdd = 0.0
    start_date = planting_date
    end_date = current_date + timedelta(days=3)
    print(f'Calculating GDD for lat: {lat}, lon: {lon}, from {start_date} to {end_date}')
    
    attributes = [
        'max_temperature', 'min_temperature'
    ]
    reader = DatasetReaderBuilder.create_reader(
        DatasetType.DAILY_FORECAST, lat, lon,
        start_date, end_date, attributes
    )
    reader.open()
    reader.read()
    df = reader.get_result_df()

    base, cap = find_crop_base_cap_temp(crop)
    if base is None or cap is None:
        print(f'Error: Base or cap temperature not found for crop: {crop}')
        return gdd
    
    df = _do_gdd_calculation(df, base, cap)
    # GDD is calculated from the day after planting to the current date - 1
    gdd = df['gdd'].iloc[:-4].sum()

    # # Fetch data by forecast date
    # gdd2, df2 = calculate_gdd_using_forecast_only(reader, current_date, base, cap)

    # # Replace last 4 rows with forecast data
    # df_calculated = df.copy()
    # df_calculated.loc[df_calculated.index[-4:], 'max_temperature'] = df2['max_temperature'].values
    # df_calculated.loc[df_calculated.index[-4:], 'min_temperature'] = df2['min_temperature'].values
    # df_calculated = _do_gdd_calculation(df_calculated, base, cap)
    # gdd_calculated = df_calculated['gdd'].sum()

    reader.close()

    return gdd, df

def calculate_gdd_using_forecast_only(reader, current_date, base, cap):
    """Calculate Growing Degree Days (GDD) for a given latitude and longitude.

    Args:
        lat (float): Latitude of the location.
        lon (float): Longitude of the location.
        start_date (datetime.date): Start date for GDD calculation.
        end_date (datetime.date): End date for GDD calculation.
    Returns:
        float: Calculated GDD value.
    """
    gdd = 0.0
    start_date = current_date
    end_date = current_date + timedelta(days=3)

    reader.start_date = start_date
    reader.end_date = end_date
    print(f'Fetch data by forecast date from {start_date} to {end_date}')
    reader.read_by_forecast_date(current_date)
    df = reader.get_result_df()

    return gdd, df
