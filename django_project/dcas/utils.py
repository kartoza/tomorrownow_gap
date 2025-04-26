# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Utilities
"""

import pandas as pd
import duckdb


def read_grid_data(
    parquet_file_path, column_list: list, grid_id_list: list,
    num_threads = None
) -> pd.DataFrame:
    """Read grid data from parquet file.

    :param parquet_file_path: file_path to parquet file
    :type parquet_file_path: str
    :param column_list: List of column to be read
    :type column_list: list
    :param grid_id_list: List of grid_id to be filtered
    :type grid_id_list: list
    :param num_threads: number of threads for duck db
    :type num_threads: int
    :return: DataFrame that contains grid_id and column_list
    :rtype: pd.DataFrame
    """
    config = {}
    if num_threads is not None:
        config['threads'] = num_threads
    conndb = duckdb.connect(config=config)
    query = (
        f"""
        SELECT {','.join(column_list)}
        FROM read_parquet('{parquet_file_path}')
        WHERE grid_id IN {list(grid_id_list)}
        """
    )
    df = conndb.sql(query).df()
    conndb.close()
    return df


def read_grid_crop_data(
    parquet_file_path, grid_crop_keys, num_threads = None
) -> pd.DataFrame:
    """Read grid data from parquet file.

    :param parquet_file_path: file_path to parquet file
    :type parquet_file_path: str
    :param grid_crop_keys: List of unique key
    :type grid_crop_keys: list
    :param num_threads: number of threads for duck db
    :type num_threads: int
    :return: DataFrame that contains grid_id and column_list
    :rtype: pd.DataFrame
    """
    config = {}
    if num_threads is not None:
        config['threads'] = num_threads
    conndb = duckdb.connect(config=config)
    query = (
        f"""
        SELECT *
        FROM read_parquet('{parquet_file_path}')
        WHERE grid_crop_key IN {grid_crop_keys}
        """
    )
    # grid_crop_key = crop_id || '_' || crop_stage_type_id || '_' || grid_id
    df = conndb.sql(query).df()
    conndb.close()
    return df


def print_df_memory_usage(df: pd.DataFrame):
    """Print dataframe memory usage.

    :param df: dataframe
    :type df: pd.DataFrame
    """
    memory = df.memory_usage(deep=True)
    total_memory = memory.sum()  # Total memory usage in bytes

    print(f"Total memory usage: {total_memory / 1024:.2f} KB")


def get_previous_week_message(duckdb_file: str, grid_crop_keys: list):
    """
    Get the previous week's message for a given grid crop key.

    :param duckdb_file: Path to the DuckDB file.
    :type duckdb_file: str
    :param grid_crop_keys: List of grid crop keys.
    :type grid_crop_keys: list
    :return: DataFrame containing the previous week's message.
    :rtype: pd.DataFrame
    """
    conn = duckdb.connect(duckdb_file)
    query = f"""
        SELECT *
        FROM dcas
        WHERE grid_crop_key IN {grid_crop_keys}
    """
    df = conn.sql(query).df()
    conn.close()
    return df
