# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Analytics functions.
"""

import pandas as pd
import numpy as np
from datetime import timedelta, date as date_t, time, datetime
import pytz

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



def extract_dcas_error_log():
    """Extract DCAS error log."""
    date = date_t(2025, 7, 30)
    input_file = 'input/DCAS_Error_File 20250725.csv'
    dcas_error_df = read_dcas_error_log_file(input_file)

    print(dcas_error_df.head())
    farmer_ids = dcas_error_df['FarmerId'].unique().tolist()
    print(f'Number of unique farmer IDs in DCAS error log: {len(farmer_ids)}')
    dcas_df = read_dcas_geoparquet(date, farmer_ids)

    # add growth_stage col
    dcas_df['diff'] = 0
    for index, row in dcas_df.iterrows():
        in_row = dcas_error_df[
            (dcas_error_df['FarmerId'] == row['farm_unique_id']) &
            (dcas_error_df['crop'] == row['crop'])
        ]
        if not in_row.empty:
            growthStage = in_row['growthStage'].values[0]
            dcas_df.at[index, 'growth_stage_in'] = growthStage
            dcas_df.at[index, 'diff'] = 1 if growthStage != row['growth_stage'] else 0
        # else:
        #     print(f'⚠️ No growth stage found for farm_unique_id: {row["farm_unique_id"]} and crop: {row["crop"]}')

    # filter out dcas_df where growth_stage_in is not null
    dcas_df = dcas_df[dcas_df['growth_stage_in'].notnull()]
    print(f'Length of DCAS Data after filtering: {len(dcas_df)} - {len(dcas_error_df)}')
    # total diff
    total_diff = dcas_df['diff'].sum()
    print(f'Total differences found: {total_diff}')

    # write the dcas_df to excel
    dcas_df.to_excel(f'output/DCAS_DATA_{date.isoformat()}.xlsx', index=False)

    # count growth_stage values
    growth_stage_counts = dcas_df['growth_stage'].value_counts()
    print('Growth Stage Counts:')
    for stage, count in growth_stage_counts.items():
        print(f'{stage}: {count}')


def extract_dcas_gdd():
    """Extract DCAS GDD data."""
    SAMPLE_NUMBER = 10
    date = date_t(2025, 7, 30)
    grid_ref_file = 'output/grids.xlsx'
    grid_df = pd.read_excel(grid_ref_file)
    print(f'Grid columns: {grid_df.columns}')

    input_file = 'output/DCAS_DATA_2025-07-30.xlsx'
    dcas_df = pd.read_excel(input_file)
    print(f'DCAS Data Length: {len(dcas_df)}')
    print(f'DCAS Data Columns: {dcas_df.columns}')

    # Step 1: Pick one random row per unique grid_unique_id
    unique_grids = dcas_df.groupby('grid_unique_id').sample(n=1)
    n_unique = dcas_df['grid_unique_id'].nunique()
    print(f'Number of unique grid_unique_id: {n_unique}')
    n_pick = min(SAMPLE_NUMBER, n_unique)

    # result_df = unique_grids.sample(n=n_pick)

    farmer_ids_picked = [
        'uuid:c63e2d42-ef02-4d99-9b47-06c9dd5d0939',
        'uuid:19f1f033-d5f9-425b-b9ac-84a19ad7eca0',
        'uuid:862ecafb-145c-4c3c-8405-1cbf0b30df9e'
    ]
    result_df = dcas_df[dcas_df['farm_unique_id'].isin(farmer_ids_picked)]

    # Convert epoch planting_date_epoch column to date column
    if 'planting_date' not in result_df.columns:
        result_df['planting_date'] = pd.to_datetime(
            result_df['planting_date_epoch'],
            unit='s'
        ).dt.date

    # Add grid_lat and grid_lon columns
    result_df['grid_lat'] = np.nan
    result_df['grid_lon'] = np.nan

    # Step 2: Merge with grid_df to get lat and lon
    not_found_count = 0
    for index, row in result_df.iterrows():
        grid_id = row['grid_unique_id']
        grid_row = grid_df[grid_df['unique_id'] == grid_id]
        if not grid_row.empty:
            result_df.at[index, 'grid_lat'] = grid_row['lat'].values[0]
            result_df.at[index, 'grid_lon'] = grid_row['lon'].values[0]
        else:
            not_found_count += 1
            print(f'⚠️ Grid ID {grid_id} not found in grid_df.')

    if not_found_count == 0:
        print('✅ All grid_unique_id found in grid_df.')

    select_columns = [
        'farm_unique_id',
        'planting_date',
        'crop',
        'grid_unique_id',
        'grid_lat',
        'grid_lon',
        'growth_stage',
        'total_gdd'
    ]
    print(result_df[select_columns].head())

    # Step 3: Calculate GDD for each farm
    for index, row in result_df.iterrows():
        lat = row['grid_lat']
        lon = row['grid_lon']
        planting_date = row['planting_date']
        if isinstance(planting_date, date_t):
            planting_date = datetime.combine(planting_date, time.min, tzinfo=pytz.UTC)
        current_date = datetime(
            date.year, date.month, date.day, 0, 0, 0, tzinfo=pytz.UTC
        )
        crop = row['crop']

        gdd, df = calculate_gdd(lat, lon, planting_date, current_date, crop)
        # add some columns
        df['farmer_id'] = row['farm_unique_id']
        df['grid_id'] = row['grid_unique_id']
        df['lat'] = row['grid_lat']
        df['lon'] = row['grid_lon']
        df['planting_date'] = planting_date.date()
        df['crop'] = crop

        result_df.at[index, 'gdd_calc'] = gdd

        # Step 4: Save the result to an Excel file
        output_file = f'output/gdd/DCAS_GDD_{row["farm_unique_id"]}_{date.isoformat()}.xlsx'
        df.to_excel(output_file, index=False)
        print(f'GDD data saved to {output_file}')

    # Also save result_df to an Excel file
    result_output_file = f'output/DCAS_GDD_RESULT_{date.isoformat()}.xlsx'
    result_df.to_excel(result_output_file, index=False)
    print(f'Result data saved to {result_output_file}')


def main():
    """Main function to run DCAS analytics."""
    # extract_dcas_error_log()

    # farm_id = '12345'
    # lat = 1.26728
    # lon = 35.336
    # planting_date = datetime.datetime(2025, 4, 20, 0, 0, 0, tzinfo=pytz.UTC)
    # current_date = datetime.datetime(2025, 7, 25, 0, 0, 0, tzinfo=pytz.UTC)
    # crop = 'Maize_Mid'
    # gdd, df = calculate_gdd(lat, lon, planting_date, current_date, crop)
    # print(f'Calculated GDD: {gdd}')
    # print(df.tail())
    # # extract to excel
    # df.to_excel(f'output/GDD_DATA_{farm_id}.xlsx', index=False)
    # print(f'GDD data saved to output/GDD_DATA_{farm_id}.xlsx')
    extract_dcas_gdd()
