# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Functions to process partitions.
"""

import pandas as pd
import numpy as np

from gap.models import Attribute
from dcas.models import GDDConfig
from dcas.rules.rule_engine import DCASRuleEngine
from dcas.utils import read_grid_data, read_grid_crop_data
from dcas.functions import (
    calculate_growth_stage,
    calculate_message_output
)


def process_partition_total_gdd(
    df: pd.DataFrame, parquet_file_path: str, epoch_list: list
) -> pd.DataFrame:
    """Calculate cumulative sum of GDD for each day.

    :param df: DataFrame partition to be processed
    :type df: pd.DataFrame
    :param parquet_file_path: parquet that has max and min temperature
    :type parquet_file_path: str
    :param epoch_list: List of epoch in current process
    :type epoch_list: list
    :return: DataFrame with GDD cumulative sum for each day columns
    :rtype: pd.DataFrame
    """
    grid_column_list = ['grid_id', 'config_id']
    for epoch in epoch_list:
        grid_column_list.append(f'max_temperature_{epoch}')
        grid_column_list.append(f'min_temperature_{epoch}')

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(
        parquet_file_path, grid_column_list, grid_id_list
    )

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    # merge with base and cap temperature in gdd config
    df = _merge_partition_gdd_config(df)

    # add new column to normalize max and min temperature
    norm_temperature = {}
    for epoch in epoch_list:
        norm_temperature[f'max_temperature_{epoch}'] = (
            df[f'max_temperature_{epoch}'].clip(upper=df['gdd_cap'])
        )
        norm_temperature[f'min_temperature_{epoch}'] = (
            df[f'min_temperature_{epoch}'].clip(lower=df['gdd_base'])
        )

    norm_temperature_df = pd.DataFrame(norm_temperature)

    # calculate gdd foreach day from planting_date
    gdd_cols = []
    gdd_dfs = {}
    for epoch in epoch_list:
        c_name = f'gdd_{epoch}'
        gdd_dfs[c_name] = np.where(
            df['planting_date_epoch'] > epoch,
            np.nan,
            (
                (
                    norm_temperature_df[f'max_temperature_{epoch}'] +
                    norm_temperature_df[f'min_temperature_{epoch}']
                ) / 2) - df['gdd_base']
        )
        gdd_cols.append(c_name)

    gdd_temp_df = pd.DataFrame(gdd_dfs)

    # Calculate cumulative sum for each gdd column
    cumsum_gdd_df = gdd_temp_df.cumsum(axis=1)
    cumsum_gdd_df.columns = [f"gdd_sum_{epoch}" for epoch in epoch_list]

    # data cleanup
    grid_column_list.remove('grid_id')
    grid_column_list.remove('config_id')
    grid_column_list.append('gdd_base')
    grid_column_list.append('gdd_cap')
    df = df.drop(columns=grid_column_list)

    # combine df with cumulative sum of gdd
    df = pd.concat([df, cumsum_gdd_df], axis=1)

    return df


def process_partition_seasonal_precipitation(
    df: pd.DataFrame, parquet_file_path: str, epoch_list: list
) -> pd.DataFrame:
    """Calculate seasonal precipitation parameter.

    :param df: DataFrame partition to be processed
    :type df: pd.DataFrame
    :param parquet_file_path: parquet that has total_rainfall
    :type parquet_file_path: str
    :param epoch_list: List of epoch in current process
    :type epoch_list: list
    :return: DataFrame with seasonal_precipitation column
    :rtype: pd.DataFrame
    """
    grid_column_list = ['grid_id']
    for epoch in epoch_list:
        grid_column_list.append(f'total_rainfall_{epoch}')

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(
        parquet_file_path, grid_column_list, grid_id_list
    )

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    # calculate seasonal_precipitation
    grid_column_list.remove('grid_id')
    df['seasonal_precipitation'] = df[grid_column_list].sum(axis=1)

    # data cleanup
    df = df.drop(columns=grid_column_list)

    return df


def process_partition_other_params(
    df: pd.DataFrame, parquet_file_path: str
) -> pd.DataFrame:
    """Merge temperature, humidity, and p_pet to current DataFrame.

    :param df: DataFrame partition to be processed
    :type df: pd.DataFrame
    :param parquet_file_path: parquet that has temperature, humidity and p_pet
    :type parquet_file_path: str
    :return: DataFrame with temperature, humidity, and p_pet columns.
    :rtype: pd.DataFrame
    """
    grid_column_list = ['grid_id', 'temperature', 'humidity', 'p_pet']

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(
        parquet_file_path, grid_column_list, grid_id_list
    )

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    return df


def process_partition_growth_stage(
    df: pd.DataFrame, epoch_list: list
) -> pd.DataFrame:
    """Calculate growth_stage and its start date for df partition.

    :param df: DataFrame partition to be processed
    :type df: pd.DataFrame
    :param epoch_list: list of epoch
    :type epoch_list: list
    :return: DataFrame with growth_stage_id and
        growth_stage_start_date columns
    :rtype: pd.DataFrame
    """
    last_gdd_epoch = epoch_list[-1]
    df = df.assign(
        growth_stage_start_date=pd.Series(dtype='double'),
        growth_stage_id=pd.Series(dtype='int'),
        total_gdd=df[f'gdd_sum_{last_gdd_epoch}']
    )

    df = df.apply(
        calculate_growth_stage,
        axis=1,
        args=(epoch_list,)
    )

    return df


def process_partition_growth_stage_precipitation(
    df: pd.DataFrame, parquet_file_path: str, epoch_list: list
) -> pd.DataFrame:
    """Calculate growth_stage_percipitation for df partition.

    :param df: DataFrame partition to be processed
    :type df: pd.DataFrame
    :param parquet_file_path: parquet with total_rainfall data
    :type parquet_file_path: str
    :param epoch_list: List of epoch in current process
    :type epoch_list: list
    :return: DataFrame with growth_stage_precipitation column
    :rtype: pd.DataFrame
    """
    grid_column_list = ['grid_id']
    for epoch in epoch_list:
        grid_column_list.append(f'total_rainfall_{epoch}')

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(
        parquet_file_path, grid_column_list, grid_id_list
    )

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    for epoch in epoch_list:
        c_name = f'total_rainfall_{epoch}'
        df[c_name] = np.where(
            df['growth_stage_start_date'] > epoch,
            np.nan,
            df[c_name]
        )

    grid_column_list.remove('grid_id')
    df['growth_stage_precipitation'] = df[grid_column_list].sum(axis=1)

    # data cleanup
    df = df.drop(columns=grid_column_list)

    return df


def process_partition_message_output(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate message codes for DataFrame partition.

    :param df: DataFrame partition to be processed
    :type df: pd.DataFrame
    :return: DataFrame with message columns
    :rtype: pd.DataFrame
    """
    df = df.assign(
        message=None,
        message_2=None,
        message_3=None,
        message_4=None,
        message_5=None
    )

    attrib_dict = {
        'temperature': Attribute.objects.get(variable_name='temperature').id,
        'humidity': Attribute.objects.get(
            variable_name='relative_humidity'
        ).id,
        'p_pet': Attribute.objects.get(variable_name='p_pet').id,
        'growth_stage_precipitation': Attribute.objects.get(
            variable_name='growth_stage_precipitation'
        ).id,
        'seasonal_precipitation': Attribute.objects.get(
            variable_name='seasonal_precipitation'
        ).id
    }

    rule_engine = DCASRuleEngine()
    rule_engine.initialize()

    df = df.apply(
        calculate_message_output,
        axis=1,
        args=(rule_engine, attrib_dict,)
    )

    return df


def _merge_partition_gdd_config(df: pd.DataFrame) -> pd.DataFrame:
    """Merge dataframe with GDD config: base and cap temperature.

    :param df: input DataFrame that has column: config_id and crop_id
    :type df: pd.DataFrame
    :return: dataframe with new columns: gdd_base, gdd_cap
    :rtype: pd.DataFrame
    """
    crop_list = []
    config_list = []
    base_list = []
    cap_list = []
    configs = GDDConfig.objects.all().order_by('config_id', 'crop_id')
    for gdd_config in configs:
        config_list.append(gdd_config.config.id)
        crop_list.append(gdd_config.crop.id)
        base_list.append(gdd_config.base_temperature)
        cap_list.append(gdd_config.cap_temperature)

    gdd_config_df = pd.DataFrame({
        'crop_id': crop_list,
        'config_id': config_list,
        'gdd_base': base_list,
        'gdd_cap': cap_list
    })

    return df.merge(gdd_config_df, how='inner', on=['crop_id', 'config_id'])


def process_partition_farm_registry(
    df: pd.DataFrame, parquet_file_path: str, growth_stage_mapping: dict
) -> pd.DataFrame:
    """Merge farm registry dataframe with grid crop data.

    :param df: farm registry dataframe
    :type df: pd.DataFrame
    :param parquet_file_path: parquet to grid crop data
    :type parquet_file_path: str
    :param growth_stage_mapping: dict mapping of growthstage label
    :type growth_stage_mapping: dict
    :return: merged dataframe
    :rtype: pd.DataFrame
    """
    grid_id_list = df['grid_id'].unique()
    crop_id_list = df['crop_id'].unique()
    crop_stage_type_list = df['crop_stage_type_id'].unique()

    # read grid_data_df
    grid_data_df = read_grid_crop_data(
        parquet_file_path, grid_id_list,
        crop_id_list, crop_stage_type_list
    )

    grid_data_df = grid_data_df.drop(
        columns=['__null_dask_index__', 'planting_date']
    )

    # merge the df with grid_data
    df = df.merge(
        grid_data_df,
        on=[
            'grid_id', 'crop_id', 'crop_stage_type_id',
            'planting_date_epoch'
        ],
        how='inner'
    )

    df['growth_stage'] = df['growth_stage_id'].map(growth_stage_mapping)

    return df
