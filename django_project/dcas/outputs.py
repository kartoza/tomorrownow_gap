# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Outputs
"""

import paramiko
import os
import shutil
import fsspec
import pandas as pd
from dask.dataframe.core import DataFrame as dask_df
import dask_geopandas as dg
from dask_geopandas.io.parquet import to_parquet
from typing import Union
import duckdb
from django.conf import settings

from core.utils.file import format_size
from gap.utils.dask import execute_dask_compute


class OutputType:
    """Enum class for output type."""

    GRID_DATA = 1
    GRID_CROP_DATA = 2
    FARM_CROP_DATA = 3
    MESSAGE_DATA = 4


class DCASPipelineOutput:
    """Class to manage pipeline output."""

    TMP_BASE_DIR = '/tmp/dcas'
    DCAS_OUTPUT_DIR = 'dcas_output'

    def __init__(
        self, request_date, extract_additional_columns=False,
        duck_db_num_threads=None
    ):
        """Initialize DCASPipelineOutput."""
        self.fs = None
        self.request_date = request_date
        self.extract_additional_columns = extract_additional_columns
        self.duck_db_num_threads = duck_db_num_threads

    def setup(self):
        """Set DCASPipelineOutput."""
        self._setup_s3fs()

        # clear temp resource
        if os.path.exists(self.TMP_BASE_DIR):
            shutil.rmtree(self.TMP_BASE_DIR)
        os.makedirs(self.TMP_BASE_DIR, exist_ok=True)

    def cleanup(self):
        """Remove temporary directory."""
        if os.path.exists(self.TMP_BASE_DIR):
            shutil.rmtree(self.TMP_BASE_DIR)

    @property
    def grid_data_file_path(self):
        """Return full path to grid data output parquet file."""
        return os.path.join(
            self.TMP_BASE_DIR,
            'grid_data.parquet'
        )

    @property
    def grid_crop_data_dir_path(self):
        """Return full path to directory grid with crop data."""
        return os.path.join(
            self.TMP_BASE_DIR,
            'grid_crop'
        )

    @property
    def grid_crop_data_path(self):
        """Return full path to grid with crop data."""
        return self.grid_crop_data_dir_path + '/*.parquet'

    @property
    def output_csv_file_path(self):
        """Return full path to output csv file."""
        dt = self.request_date.strftime('%Y%m%d')
        return os.path.join(
            self.TMP_BASE_DIR,
            f'output_{dt}.csv'
        )

    def _setup_s3fs(self):
        """Initialize s3fs."""
        self.s3 = self._get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self._get_s3_client_kwargs()
        }
        self.fs = fsspec.filesystem(
            's3',
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=self._get_s3_client_kwargs()
        )

    def _get_s3_variables(self) -> dict:
        """Get s3 env variables for product bucket.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        prefix = 'MINIO'
        keys = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'AWS_ENDPOINT_URL', 'AWS_REGION_NAME'
        ]
        results = {}
        for key in keys:
            results[key] = os.environ.get(f'{prefix}_{key}', '')
        results['AWS_BUCKET_NAME'] = os.environ.get(
            'MINIO_GAP_AWS_BUCKET_NAME', '')
        results['AWS_DIR_PREFIX'] = os.environ.get(
            'MINIO_GAP_AWS_DIR_PREFIX', '')

        return results

    def _get_s3_client_kwargs(self) -> dict:
        """Get s3 client kwargs for parquet file.

        :return: dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        prefix = 'MINIO'
        client_kwargs = {}
        if os.environ.get(f'{prefix}_AWS_ENDPOINT_URL', ''):
            client_kwargs['endpoint_url'] = os.environ.get(
                f'{prefix}_AWS_ENDPOINT_URL', '')
        if os.environ.get(f'{prefix}_AWS_REGION_NAME', ''):
            client_kwargs['region_name'] = os.environ.get(
                f'{prefix}_AWS_REGION_NAME', '')
        return client_kwargs

    def _get_directory_path(self, directory_name):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/{directory_name}"
        )

    def save(self, type: int, df: Union[pd.DataFrame, dask_df]):
        """Save output to parquet files.

        :param type: Type of the dataframe output
        :type type: int
        :param df: DataFrame output
        :type df: Union[pd.DataFrame, dask_df]
        :raises ValueError: Raise when there is invalid type
        """
        if type == OutputType.GRID_DATA:
            self._save_grid_data(df)
        elif type == OutputType.GRID_CROP_DATA:
            self._save_grid_crop_data(df)
        elif type == OutputType.FARM_CROP_DATA:
            self._save_farm_crop_data(df)
        else:
            raise ValueError(f'Invalid output type {type} to be saved!')

    def _save_farm_crop_data(self, df: dask_df):
        df_geo = dg.from_dask_dataframe(
            df,
            geometry=dg.from_wkb(df['geometry'])
        )

        print('Saving to parquet')

        x = to_parquet(
            df_geo,
            self._get_directory_path(self.DCAS_OUTPUT_DIR),
            partition_on=['iso_a3', 'year', 'month', 'day'],
            filesystem=self.fs,
            compute=False
        )
        print(f'writing to {self._get_directory_path(self.DCAS_OUTPUT_DIR)}')
        execute_dask_compute(x)

    def _save_grid_crop_data(self, df: dask_df):
        dir_path = self.grid_crop_data_dir_path
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)

        print('Saving to parquet')

        df = df.reset_index(drop=True)
        x = df.to_parquet(
            dir_path,
            compute=False
        )
        print(f'writing to {dir_path}')
        execute_dask_compute(x)

    def _save_grid_data(self, df: pd.DataFrame):
        file_path = self.grid_data_file_path
        print(f'writing dataframe to {file_path}')
        df.to_parquet(file_path)

    def _upload_to_sftp(self, local_file):
        """Upload CSV file to Docker SFTP."""
        try:
            print(f'Connecting to SFTP server at '
                  f'{settings.SFTP_HOST}:{settings.SFTP_PORT}...')
            transport = paramiko.Transport(
                (settings.SFTP_HOST, settings.SFTP_PORT)
            )
            transport.connect(
                username=settings.SFTP_USERNAME,
                password=settings.SFTP_PASSWORD
            )

            sftp = paramiko.SFTPClient.from_transport(transport)

            # Ensure correct remote path
            remote_file_path = (
                f"{settings.SFTP_REMOTE_PATH}/{os.path.basename(local_file)}"
            )
            print(f"Uploading {local_file} to {remote_file_path}...")

            sftp.put(local_file, remote_file_path)  # Upload file

            print("Upload to Docker SFTP successful!")

            # Close connection
            sftp.close()
            transport.close()

        except Exception as e:
            print(f"Failed to upload to SFTP: {e}")

    def _get_connection(self, s3):
        endpoint = s3['AWS_ENDPOINT_URL']
        if settings.DEBUG:
            endpoint = endpoint.replace('http://', '')
        else:
            endpoint = endpoint.replace('https://', '')
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]

        config = {
            's3_access_key_id': s3['AWS_ACCESS_KEY_ID'],
            's3_secret_access_key': s3['AWS_SECRET_ACCESS_KEY'],
            's3_region': 'us-east-1',
            's3_url_style': 'path',
            's3_endpoint': endpoint,
            's3_use_ssl': not settings.DEBUG
        }
        if self.duck_db_num_threads:
            config['threads'] = self.duck_db_num_threads

        conn = duckdb.connect(config=config)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        return conn

    def convert_to_csv(self):
        """Convert output to csv file."""
        file_path = self.output_csv_file_path
        column_list = [
            'farm_unique_id as farmer_id', 'crop',
            'planting_date as plantingDate', 'growth_stage as growthStage',
            'message', 'message_2', 'message_3', 'message_4', 'message_5',
            'humidity as relativeHumidity',
            'seasonal_precipitation as seasonalPrecipitation',
            'temperature', 'p_pet as PPET',
            'growth_stage_precipitation as growthStagePrecipitation'
        ]
        if self.extract_additional_columns:
            column_list.extend([
                "total_gdd", "grid_id",
                "strftime(to_timestamp(growth_stage_start_date)" +
                ", '%Y-%m-%d') as growth_stage_date"
            ])

        parquet_path = (
            f"'{self._get_directory_path(self.DCAS_OUTPUT_DIR)}/"
            "iso_a3=*/year=*/month=*/day=*/*.parquet'"
        )
        s3 = self._get_s3_variables()
        conn = self._get_connection(s3)
        sql = (
            f"""
            SELECT {','.join(column_list)}
            FROM read_parquet({parquet_path}, hive_partitioning=true)
            WHERE year={self.request_date.year} AND
            month={self.request_date.month} AND
            day={self.request_date.day}
            """
        )
        final_query = (
            f"""
            COPY({sql})
            TO '{file_path}'
            (HEADER, DELIMITER ',');
            """
        )
        print(f'Extracting csv to {file_path}')
        conn.sql(final_query)
        conn.close()

        file_stats = os.stat(file_path)
        print(
            f'Extracted csv {file_path} file size: '
            f'{format_size(file_stats.st_size)}'
        )

        return file_path
