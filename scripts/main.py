# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Analytics main function.
"""

import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pytz

from analytics.spw import fetch_spw_data, read_spw_geoparquet_by_farm_group
from analytics.farmers import read_excel_stats, read_excel_farmers
from analytics.dcas import (
    read_dcas_geoparquet,
    read_dcas_error_log_file,
    calculate_gdd
)
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


def plot_spw_data(date):
    """Plot SPW data."""
    farmer_df = read_excel_farmers()
    farmer_df = farmer_df[['farmer_id', 'gps_latitude', 'gps_longitude']]
    print(f'Length of Farmer List: {len(farmer_df)}')

    spw_df = fetch_spw_data(date, farmer_df)

    # rename farm_unique_id to farmer_id
    print(f'Length of SPW Data: {len(spw_df)}')
    # create spw_signal column with value:
    # 1 if signal has 'Plant NOW' in it,
    # 0 if signal has 'Do NOT plant' in it,
    spw_df['spw_signal'] = spw_df['signal'].apply(lambda x: 1 if 'Plant NOW' in x else 0)
    spw_df['spw_signal'] = spw_df['spw_signal'].astype(int)
    print(spw_df.columns)

    # export to excel for debugging
    spw_df.to_excel(f'output/SPW_DATA_PLOT_{date.isoformat()}.xlsx', index=False)

    print(str('spw_signal'),np.nansum(spw_df['spw_signal']))
    # plot the spw_signal column
    plt.scatter(spw_df['gps_longitude'], spw_df['gps_latitude'], c=spw_df['spw_signal'].values)
    plt.title(f"SPW Signal Plot - {date.isoformat()}")
    plt.show()


def plot_spw_with_tamsat(date):
    """Plot SPW data with TAMSAT.
    
    Notes:
        - Tamsat spw csv file should be in the output folder with name SPW_KALRO_<date>.csv
        - SPW data should be in the output folder with name SPW_DATA_PLOT_<date>.xlsx
    """
    print(f'Plotting SPW data with TAMSAT for date: {date.isoformat()}')
    spw_file_path = f'output/SPW_DATA_PLOT_{date.isoformat()}.xlsx'
    tamsat_file_path = f'output/SPW_KALRO_{date.isoformat()}.csv'

    spw_df = pd.read_excel(spw_file_path)
    # Rename gps_longitude and gps_latitude to Longitude and Latitude
    spw_df.rename(columns={'gps_longitude': 'Longitude', 'gps_latitude': 'Latitude'}, inplace=True)
    tamsat_df = pd.read_csv(tamsat_file_path)

    # Print stats
    print('--- SPW Data Stats ---')
    print(f'SPW Data Length: {len(spw_df)}')
    print('spw_signal',np.nansum(spw_df['spw_signal']))
    print('--- TAMSAT Data Stats ---')
    print(f'TAMSAT Data Length: {len(tamsat_df)}')
    tamsat_spw_cols = [
        'spw_20', 'spw_40', 'spw_60',
        'sm_25', 'sm_50', 'sm_70'
    ]
    for col in tamsat_spw_cols:
        if col not in tamsat_df.columns:
            print(f'Column {col} not found in TAMSAT data.')
            continue
        print(f'{col}: {np.nansum(tamsat_df[col])}')

    # Create a figure with 3 row, 3 columns of subplots
    fig, axes = plt.subplots(3, 3, figsize=(15, 12), sharex=True, sharey=True)

    # construct array of df
    dfs = []
    for col in tamsat_spw_cols:
        if col not in tamsat_df.columns:
            print(f'Column {col} not found in TAMSAT data.')
            continue
        # create a new dataframe with spw_signal and the tamsat column
        temp_df = tamsat_df[['Longitude', 'Latitude', col]].copy()
        # rename col to 'spw_signal'
        temp_df.rename(columns={col: 'spw_signal'}, inplace=True)
        dfs.append(temp_df)
    # append the spw_df with spw_signal column
    dfs.append(spw_df)

    # Flatten axes into a 1D list so we can loop easily
    axes_list = axes.ravel()
    for i in range(9):  # total 9 subplot slots
        if i < len(dfs):
            df = dfs[i]
            axes_list[i].scatter(
                df['Longitude'],
                df['Latitude'],
                c=df['spw_signal'].values
            )
            axes_list[i].set_title(tamsat_spw_cols[i] if i < len(tamsat_spw_cols) else 'KTZ')
            axes_list[i].set_xlabel("Longitude")
            axes_list[i].set_ylabel("Latitude")
        else:
            # Hide unused subplots
            axes_list[i].axis('off')

    fig.suptitle(f"SPW Data - {date.isoformat()}", fontsize=16)
    plt.tight_layout()
    plt.show()


def extract_dcas_error_log():
    """Extract DCAS error log."""
    date = datetime.date(2025, 7, 30)
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
    date = datetime.date(2025, 7, 30)
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
        if isinstance(planting_date, datetime.date):
            planting_date = datetime.datetime.combine(planting_date, datetime.time.min, tzinfo=pytz.UTC)
        current_date = datetime.datetime(
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


if __name__ == "__main__":
    start_date = datetime.date(2025, 4, 30)
    # end_date = datetime.date(2025, 4, 25)
    # current_date = start_date
    # while current_date <= end_date:
    #     print(f'Processing date: {current_date}')
    #     pull_spw_data(current_date)
    #     current_date += datetime.timedelta(days=1)

    # plot_spw_with_tamsat(start_date)
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
