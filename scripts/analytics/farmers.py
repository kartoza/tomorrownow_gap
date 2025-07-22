# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW farmers functions.
"""

import pandas as pd

from analytics.fixtures import SPW_MESSAGE_DICT


def read_excel_stats(sheet_name):
    """Read SPW statistics from an Excel file.

    Args:
        sheet_name (str): Name of the sheet to read.
    Returns:
        pd.DataFrame: DataFrame containing the SPW statistics.
    """

    input_file = 'input/SPW_STATS.xlsx'
    # input_file = 'input/SPW_STATS_27March.xlsx'
    df = pd.read_excel(input_file, sheet_name=sheet_name)
    # filter out rows where 'farmer_id' is NA
    df = df[df['farmer_id'].notna()]
    df['farmer_id'] = df['farmer_id'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    message_mapping = {}
    for key, value in SPW_MESSAGE_DICT.items():
        combined_message = f"{value['message']} - {value['description']}"
        message_mapping[combined_message] = key

    # remove duplicate spaces in 'SPWTopMessage' and 'SPWDescription'
    df['SPWTopMessage'] = df['SPWTopMessage'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df['SPWDescription'] = df['SPWDescription'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # Map new column: sent_signal based on SPWTopMessage and SPWDescription with lookup from SPW_MESSAGE_DICT
    df['sent_signal'] = df.apply(lambda x: message_mapping.get(f"{x['SPWTopMessage']} - {x['SPWDescription']}", None), axis=1)

    return df


def read_excel_farmers():
    """Read farmers from an Excel file.

    Returns:
        pd.DataFrame: DataFrame containing the farmers.
    """
    input_file = 'input/SPWDATA_Result.xls'
    df = pd.read_excel(input_file)
    # filter out rows where 'farmer_id' is NA
    df = df[df['farmer_id'].notna()]
    df['farmer_id'] = df['farmer_id'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    return df
