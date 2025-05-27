# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Queries functions.
"""

import os
import re
import datetime
import uuid
from mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine
from django.core.files.storage import storages

from core.settings.utils import absolute_path
from dcas.tests.base import DCASPipelineBaseTest
from dcas.pipeline import DCASDataPipeline
from dcas.outputs import DCASPipelineOutput
from dcas.queries import DataQuery


class DCASQueriesTest(DCASPipelineBaseTest):
    """DCAS Queries test case."""

    @patch('dcas.queries.duckdb.connect')
    def test_read_grid_data_crop_meta_parquet(self, mock_duckdb_connect):
        """Test read_grid_data_crop_meta_parquet function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        data_query = DataQuery()

        data_query.read_grid_data_crop_meta_parquet('/tmp/dcas/grid_crop')
        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_get_farms_without_messages_chunked(self):
        """Test retrieving farms with missing messages in chunks."""
        # Mock DuckDB return DataFrames (Simulating chunked retrieval)
        chunk_1 = pd.DataFrame({'farm_id': [1, 2], 'crop_id': [101, 102]})
        chunk_2 = pd.DataFrame({'farm_id': [3, 4], 'crop_id': [103, 104]})

        expected_chunks = [chunk_1, chunk_2]

        # Configure mock connection to return chunks in order
        mock_conn = MagicMock()
        mock_conn.sql.return_value.df.side_effect = expected_chunks

        # Call the function
        result_chunks = list(
            DataQuery.get_farms_without_messages(
                datetime.date(2025, 1, 1),
                "/tmp/dcas/farm_crop.parquet",
                mock_conn,
                chunk_size=2
            )
        )

        # Ensure we receive correct number of chunks
        self.assertEqual(len(result_chunks), len(expected_chunks))

        # Validate each chunk
        for result_df, expected_df in zip(result_chunks, expected_chunks):
            pd.testing.assert_frame_equal(result_df, expected_df)

        # Check DuckDB Query
        expected_query_pattern = re.compile(
            r"SELECT farm_id, crop, farm_unique_id, growth_stage "
            r"FROM read_parquet\('/tmp/dcas/farm_crop.parquet', "
            r"hive_partitioning=true\) "
            r"WHERE message IS NULL "
            r"AND message_2 IS NULL "
            r"AND message_3 IS NULL "
            r"AND message_4 IS NULL "
            r"AND message_5 IS NULL "
            r"AND year=2025 AND month=1 AND "
            r"day=1 "
            r"ORDER BY registry_id "
            r"(\s+LIMIT\s+\d+\s+OFFSET\s+\d+)?"
        )

        actual_query = " ".join(mock_conn.sql.call_args[0][0].split())

        # Assert query structure matches, ignoring chunking additions
        self.assertRegex(actual_query, expected_query_pattern)

        mock_conn.close.assert_called_once()

    def test_grid_data_with_crop_meta(self):
        """Test grid_data_with_crop_meta functions."""
        pipeline = DCASDataPipeline(
            [self.farm_registry_group.id], self.request_date
        )
        conn_engine = create_engine(pipeline._conn_str())
        pipeline.data_query.setup(conn_engine)
        df = pipeline.data_query.grid_data_with_crop_meta(
            [self.farm_registry_group.id]
        )
        self.assertIn('crop_id', df.columns)
        self.assertIn('crop_stage_type_id', df.columns)
        self.assertIn('grid_id', df.columns)
        self.assertIn('grid_crop_key', df.columns)
        conn_engine.dispose()

    def test_fetch_previous_week_no_file(self):
        """Test fetch_previous_week_message function."""
        output_dir = f'tmp/dcas_output_{uuid.uuid4().hex}'
        working_dir = f'tmp/{uuid.uuid4().hex}'
        os.makedirs(working_dir, exist_ok=True)
        # init output
        s3_storage = storages['gap_products']
        dcas_output = DCASPipelineOutput(
            datetime.date(2025, 4, 22),
            duck_db_num_threads=2
        )
        dcas_output._setup_s3fs()
        s3 = dcas_output.s3

        # Call the function
        db_config = dcas_output._get_duckdb_config(s3)
        db_config['s3_use_ssl'] = False
        data_query = DataQuery()
        result_path = data_query.fetch_previous_week_message(
            f's3://{s3_storage.bucket_name}/{output_dir}',
            datetime.date(2025, 4, 22),
            working_dir,
            db_config
        )
        self.assertIsNone(result_path)
        self.assertFalse(
            os.path.exists(
                os.path.join(working_dir, 'dcas_prev_week.duckdb')
            )
        )
        # Clean up
        os.removedirs(working_dir)

    def test_fetch_previous_week_message(self):
        """Test fetch_previous_week_message function."""
        parquet_path = absolute_path(
            'dcas', 'tests', 'data', 'dcas_test.parquet'
        )
        output_dir = f'tmp/dcas_output_{uuid.uuid4().hex}'
        working_dir = f'tmp/{uuid.uuid4().hex}'
        os.makedirs(working_dir, exist_ok=True)
        s3_path = (
            f'{output_dir}/iso_a3=KEN/year=2025/month=4/day=22/part.0.parquet'
        )

        # upload test parquet file to s3
        s3_storage = storages['gap_products']
        s3_storage.save(s3_path, open(parquet_path, 'rb'))

        # init output
        dcas_output = DCASPipelineOutput(
            datetime.date(2025, 4, 22),
            duck_db_num_threads=2
        )
        dcas_output._setup_s3fs()
        s3 = dcas_output.s3

        # Call the function
        db_config = dcas_output._get_duckdb_config(s3)
        db_config['s3_use_ssl'] = False
        data_query = DataQuery()
        result_path = data_query.fetch_previous_week_message(
            f's3://{s3_storage.bucket_name}/{output_dir}',
            datetime.date(2025, 4, 22),
            working_dir,
            db_config
        )

        self.assertEqual(
            result_path,
            os.path.join(working_dir, 'dcas_prev_week.duckdb')
        )

        self.assertTrue(
            os.path.exists(
                os.path.join(working_dir, 'dcas_prev_week.duckdb')
            )
        )

        # Clean up
        os.remove(os.path.join(working_dir, 'dcas_prev_week.duckdb'))
        os.removedirs(working_dir)
        s3_storage.delete(s3_path)

    def test_fetch_previous_week_message_old_data(self):
        """Test fetch_previous_week_message function."""
        parquet_path = absolute_path(
            'dcas', 'tests', 'data', 'dcas_old.parquet'
        )
        output_dir = f'tmp/dcas_output_{uuid.uuid4().hex}'
        working_dir = f'tmp/{uuid.uuid4().hex}'
        os.makedirs(working_dir, exist_ok=True)
        s3_path = (
            f'{output_dir}/iso_a3=KEN/year=2025/month=4/day=22/part.0.parquet'
        )

        # upload test parquet file to s3
        s3_storage = storages['gap_products']
        s3_storage.save(s3_path, open(parquet_path, 'rb'))

        # init output
        dcas_output = DCASPipelineOutput(
            datetime.date(2025, 4, 22),
            duck_db_num_threads=2
        )
        dcas_output._setup_s3fs()
        s3 = dcas_output.s3

        # Call the function
        db_config = dcas_output._get_duckdb_config(s3)
        db_config['s3_use_ssl'] = False
        data_query = DataQuery()
        result_path = data_query.fetch_previous_week_message(
            f's3://{s3_storage.bucket_name}/{output_dir}',
            datetime.date(2025, 4, 22),
            working_dir,
            db_config
        )

        self.assertIsNone(result_path)

        # Clean up
        os.remove(os.path.join(working_dir, 'dcas_prev_week.duckdb'))
        os.removedirs(working_dir)
        s3_storage.delete(s3_path)
