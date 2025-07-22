# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Analytics functions.
"""

from config import get_settings
from helper.duckdb import get_duckdb_connection


def read_spw_geoparquet(date, farmer_ids):
    """Read SPW GeoParquet data for a specific month and year.

    Args:
        date (datetime.date): The date for which to fetch SPW data.
        farmer_ids (list): List of farmer IDs to filter the data.
    Returns:
        pd.DataFrame: DataFrame containing SPW data for the specified date and farmers.
    """
    settings = get_settings()
    conn = get_duckdb_connection()
    query = f"""
        SELECT * FROM read_parquet(
            's3://{settings.S3_BUCKET_NAME}/{settings.SPW_GEOPARQUET_PATH}/year={date.year}/month={date.month}.parquet',
            hive_partitioning=True
        )
        WHERE date=$1 AND farm_unique_id IN $2;
    """
    df = conn.execute(query, (date, farmer_ids)).df()

    conn.close()
    return df


def fetch_spw_data(spw_date, df):
    """Fetch SPW data from FarmID in the excel file.

    Args:
        spw_date (datetime.date): The date for which to fetch SPW data.
        df (pd.DataFrame): DataFrame containing farmer information.
    Returns:
        pd.DataFrame: DataFrame containing SPW data.
    """
    farmer_ids = df['farmer_id'].tolist()

    print(f'Reading SPW data for date: {spw_date} and farmers: {len(farmer_ids)}')
    spw_df = read_spw_geoparquet(spw_date, farmer_ids)
    print(f'Length of SPW Data: {len(spw_df)}')
    if len(spw_df) != len(df):
        print(f'⚠️ Warning: Length of SPW Data ({len(spw_df)}) does not match Farmer List ({len(df)})')
    else:
        print('✅ Length of SPW Data matches Farmer List')
    # convert farmer_id to string
    spw_df['farm_unique_id'] = spw_df['farm_unique_id'].astype(str)
    # join with df
    spw_df = df.merge(spw_df, left_on='farmer_id', right_on='farm_unique_id', how='left')

    if 'sent_signal' in spw_df.columns:
        # add new column is_diff which is True if sent_signal is different from signal column
        spw_df['is_diff'] = spw_df['sent_signal'] != spw_df['signal']

    return spw_df
