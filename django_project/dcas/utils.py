# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Utilities
"""

import logging
import pandas as pd
import duckdb
from django.core.files.storage import storages


logger = logging.getLogger(__name__)


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


def remove_dcas_output_file(file_path: str, delivery_by: str):
    """Remove dcas output file.

    :param file_path: file name to be removed
    :type file_path: str
    """
    if delivery_by != 'OBJECT_STORAGE':
        raise NotImplementedError(
            f"This function is not implemented for {delivery_by} delivery."
        )

    s3_storage = storages['gap_products']
    try:
        s3_storage.delete(file_path)
    except Exception as e:
        logger.error(
            f"Error deleting file {file_path} from S3: {str(e)}",
            exc_info=True
        )
        return False
    return True


def dcas_output_file_exists(file_path: str, delivery_by: str):
    """Check if dcas output file exists.

    :param file_path: file name to be checked
    :type file_path: str
    """
    if delivery_by != 'OBJECT_STORAGE':
        raise NotImplementedError(
            f"This function is not implemented for {delivery_by} delivery."
        )

    s3_storage = storages['gap_products']
    return s3_storage.exists(file_path)
