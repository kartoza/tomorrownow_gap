# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Analytics main function.
"""

import datetime
from analytics.spw import fetch_spw_data, read_spw_geoparquet_by_farm_group
from analytics.farmers import read_excel_stats, read_excel_farmers
from analytics.dcas import read_dcas_geoparquet, get_dcas_stats
from analytics.fixtures import SPW_MESSAGE_DICT


def compare_spw_stats():
    """Main function to compare SPW statistics."""
    date = datetime.date(2025, 3, 27)
    # spw_date = date
    spw_date = datetime.date(2025, 3, 25)
    sheet_name = '27th_March'

    farmer_df = read_excel_stats(sheet_name)
    print(f'Length of Farmer List: {len(farmer_df)}')

    df = fetch_spw_data(spw_date, farmer_df)

    # calculate percentage of differences
    total_diff = df['is_diff'].sum()
    total_rows = len(df)
    percentage_diff = (total_diff / total_rows) * 100 if total_rows > 0 else 0
    if total_diff > 0:
        print('⚠️', end=' ')
    else:
        print('✅', end=' ')
    print(f' Percentage of differences: {percentage_diff:.2f}%')
    
    display_columns = ['date', 'farm_unique_id', 'farmer_id', 'signal', 'sent_signal', 'SPWTopMessage', 'SPWDescription', 'is_diff']
    if total_diff > 0:
        print('Differences found in the data:')
        print(df[df['is_diff'] == 1][display_columns].head(10))
    else:
        print(df[display_columns].head())

    print('Data fetched and processed successfully.')

    # Add DCAS columns
    dcas_df = read_dcas_geoparquet(date, farmer_df['farmer_id'].tolist())
    
    # Merge DCAS data with SPW data if available
    if not dcas_df.empty:
        dcas_select_columns = [
            'farm_unique_id', 'planting_date', 'crop',
            'message', 'message_2', 'message_3', 'message_4', 'message_5'
        ]
        dcas_df = dcas_df[dcas_select_columns]
        # convert farm_unique_id to string for merging
        dcas_df['farm_unique_id'] = dcas_df['farm_unique_id'].astype(str)
        df = df.merge(dcas_df, on='farm_unique_id', how='left')
        print('Merged DCAS data with SPW data.')

    # remove time from date column
    df['date'] = df['date'].dt.date
    # rename columns for clarity
    df.rename(columns={
        'SPWTopMessage': 'SentSPWTopMessage',
        'SPWDescription': 'SentSPWDescription'
    }, inplace=True)
    # store the output dataframe to excel
    output_columns = [
        'date', 'farmer_id', 'farm_group', 'signal', 'sent_signal',
        'SentSPWTopMessage', 'SentSPWDescription', 'is_diff',
        'last_2_days', 'last_4_days', 'today_tomorrow', 'too_wet_indicator'
    ]
    if not dcas_df.empty:
        output_columns.extend([
            'planting_date', 'crop',
            'message', 'message_2', 'message_3', 'message_4', 'message_5'
        ])
    output_file = f'output/SPW_ANALYTICS_OUTPUT_{date.isoformat()}.xlsx'
    df[output_columns].to_excel(output_file, index=False)
    print(f'Output saved to {output_file}')


def pull_spw_data(date):
    """Main function to pull SPW data."""
    farmer_df = read_excel_farmers()
    farmer_df = farmer_df[['farmer_id', 'gps_latitude', 'gps_longitude']]
    print(f'Length of Farmer List: {len(farmer_df)}')

    df = fetch_spw_data(date, farmer_df)

    # rename farm_unique_id to farmer_id
    print(f'Length of SPW Data: {len(df)}')

    # Add columns SPWTopMessage and SPWDescription
    df['SPWTopMessage'] = df['signal'].map(lambda x: SPW_MESSAGE_DICT.get(x, {}).get('message', ''))
    df['SPWDescription'] = df['signal'].map(lambda x: SPW_MESSAGE_DICT.get(x, {}).get('description', ''))

    # pull DCAS data
    dcas_df = read_dcas_geoparquet(date, farmer_df['farmer_id'].tolist())
    dcas_columns = [
        'farm_unique_id', 'planting_date', 'crop',
        'message', 'message_2', 'message_3', 'message_4', 'message_5'
    ]
    if dcas_df.empty:
        # initialize DCAS columns if no data is found
        for col in dcas_columns:
            if col == 'farm_unique_id':
                continue
            df[col] = None
    else:
        # convert farm_unique_id to string for merging
        dcas_df['farm_unique_id'] = dcas_df['farm_unique_id'].astype(str)
        dcas_df = dcas_df[dcas_columns]
        df = df.merge(dcas_df, left_on='farmer_id', right_on='farm_unique_id', how='left')
        print('Merged DCAS data with SPW data.')

    print(df.columns)
    # remove time from date column
    df['date'] = df['date'].dt.date

    # store the output dataframe to excel
    output_columns = [
        'date', 'farmer_id', 'farm_group', 'signal',  'SPWTopMessage', 'SPWDescription',
        'last_2_days', 'last_4_days', 'today_tomorrow', 'too_wet_indicator',
        'planting_date', 'crop',
        'message', 'message_2', 'message_3', 'message_4', 'message_5'
    ]

    # save the output dataframe to excel
    output_file = f'output/SPW_DATA_OUTPUT_{date.isoformat()}.xlsx'
    df[output_columns].to_excel(output_file, index=False)
    print(f'Output saved to {output_file}')


def extract_farms():
    """Extract farms from SPW data."""
    date = datetime.date(2025, 4, 30)
    farm_groups = [
        'KALRO',
        'Laikipia Trial Site',
        'Meru Trial Site',
        'Regen organics pilot',
        'Trial site 1',
        'Trial site 2'
    ]

    for farm_group in farm_groups:
        print(f'Processing farm group: {farm_group}')
        df = read_spw_geoparquet_by_farm_group(date, farm_group)
        if df.empty:
            print(f'No data found for farm group: {farm_group}')
            continue
        
        # convert farm_unique_id to string for merging
        df['farm_unique_id'] = df['farm_unique_id'].astype(str)
        
        # store the output dataframe to excel
        print(f'Length of SPW Data for farm group {farm_group}: {len(df)}')
        # rename columns for clarity
        df.rename(columns={
            'farm_unique_id': 'FarmerID',
            'farm_group': 'FarmGroup',
            'latitude': 'Latitude',
            'longitude': 'Longitude'
        }, inplace=True)
        output_file = f'output/SPW_FARM_GROUP_{farm_group}.xlsx'
        output_columns = [
            'FarmerID', 'FarmGroup', 'Latitude', 'Longitude',
        ]
        df[output_columns].to_excel(output_file, index=False)
        print(f'Output saved to {output_file}')


if __name__ == "__main__":
    start_date = datetime.date(2025, 4, 5)
    end_date = datetime.date(2025, 4, 12)
    current_date = start_date
    while current_date <= end_date:
        print(f'Processing date: {current_date}')
        pull_spw_data(current_date)
        current_date += datetime.timedelta(days=1)
