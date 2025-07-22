# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Analytics functions.
"""

from config import get_settings
from helper.duckdb import get_duckdb_connection


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
    dcas_display_columns = [
        'date', 'farm_unique_id', 'planting_date', 'crop', 'message',
        'message_2', 'message_3', 'message_4', 'message_5'
    ]
    print(df[dcas_display_columns].head())
    if len(df) == 0:
        print('⚠️ No DCAS data found for the specified date and farmers.')
    elif len(df) != len(farmer_ids):
        print(f'⚠️ Warning: Length of DCAS Data ({len(df)}) does not match Farmer List ({len(farmer_ids)})')
    elif len(df) == len(farmer_ids):
        print('✅ Length of DCAS Data matches Farmer List')

    conn.close()
    return df
