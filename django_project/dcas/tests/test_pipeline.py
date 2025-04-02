# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline.
"""

# import uuid
# import os
from mock import patch, MagicMock
import pandas as pd
import dask.dataframe as dd
# from django.test import TransactionTestCase
from sqlalchemy import create_engine

# from gap.models import Crop, CropStageType
from dcas.models import DCASConfig, DCASConfigCountry
from dcas.pipeline import DCASDataPipeline
# from dcas.outputs import OutputType
from dcas.tests.base import DCASPipelineBaseTest
# from dcas.utils import read_grid_crop_data


def mock_function_do_nothing(df, *args, **kwargs):
    """Mock dask map_partitions do nothing."""
    return df


class DCASPipelineTest(DCASPipelineBaseTest):
    """DCAS Pipeline test case."""

    def test_merge_grid_data_with_config(self):
        """Test merge_grid_data_with_config."""
        id_list = [self.grid_1.id, self.grid_2.id]
        country_id_list = [
            self.grid_1.country.id,
            self.grid_2.country.id,
        ]
        df = pd.DataFrame({
            'country_id': country_id_list
        }, index=id_list)
        # create config for country 2
        config = DCASConfig.objects.create(name='config')
        DCASConfigCountry.objects.create(
            config=config,
            country=self.grid_2.country
        )

        pipeline = DCASDataPipeline(
            [self.farm_registry_group.id], self.request_date
        )
        df = pipeline._merge_grid_data_with_config(df)
        self.assertIn('config_id', df.columns)
        expected_df = pd.DataFrame({
            'country_id': country_id_list,
            'config_id': [self.default_config.id, config.id]
        }, index=df.index)
        expected_df['config_id'] = expected_df['config_id'].astype('Int64')
        pd.testing.assert_frame_equal(df, expected_df)

    def test_merge_grid_data_with_config_using_default(self):
        """Test merge_grid_data_with_config using default."""
        id_list = [self.grid_1.id, self.grid_2.id]
        country_id_list = [
            self.grid_1.country.id,
            self.grid_2.country.id,
        ]
        df = pd.DataFrame({
            'country_id': country_id_list
        }, index=id_list)
        pipeline = DCASDataPipeline(
            [self.farm_registry_group.id], self.request_date
        )
        df = pipeline._merge_grid_data_with_config(df)
        self.assertIn('config_id', df.columns)
        expected_df = pd.DataFrame({
            'country_id': country_id_list,
            'config_id': [self.default_config.id, self.default_config.id]
        }, index=df.index)
        expected_df['config_id'] = expected_df['config_id'].astype('Int64')
        pd.testing.assert_frame_equal(df, expected_df)

    def test_process_farm_registry_data(self):
        """Test running process_farm_registry_data."""
        pipeline = DCASDataPipeline(
            [self.farm_registry_group.id], self.request_date
        )
        conn_engine = create_engine(pipeline._conn_str())
        pipeline.data_query.setup(conn_engine)

        grid_crop_meta_df = pd.DataFrame({
            'crop_id': [1],
            'crop_stage_type_id': [1],
            'planting_date': ['2025-01-01'],
            'grid_id': [1],
            'planting_date_epoch': [1],
            '__null_dask_index__': [0],
            'temperature': [10],
            'grid_crop_key': ['1_1_1']
        })
        pipeline.data_query.read_grid_data_crop_meta_parquet = MagicMock(
            return_value=grid_crop_meta_df
        )
        pipeline.data_output.save = MagicMock()

        # Mock the map_partitions method
        with patch.object(
            dd.DataFrame, 'map_partitions', autospec=True
        ) as mock_map_partitions:
            # Set up the mock to call the mock function
            mock_map_partitions.side_effect = mock_function_do_nothing

            pipeline.process_farm_registry_data()

            mock_map_partitions.assert_called_once()

        pipeline.data_query.read_grid_data_crop_meta_parquet.\
            assert_called_once()
        pipeline.data_output.save.assert_called_once()
        conn_engine.dispose()


# class DCASAllPipelineTest(TransactionTestCase, BasePipelineTest):
#     """Test to run the pipeline with committed transaction."""

#     fixtures = [
#         '2.provider.json',
#         '3.station_type.json',
#         '4.dataset_type.json',
#         '5.dataset.json',
#         '6.unit.json',
#         '7.attribute.json',
#         '8.dataset_attribute.json',
#         '1.dcas_config.json',
#         '12.crop_stage_type.json',
#         '13.crop_growth_stage.json',
#         '14.crop.json',
#         '15.gdd_config.json',
#         '16.gdd_matrix.json'
#     ]

#     def setUp(self):
#         """Set DCASAllPipelineTest class."""
#         self.setup_test()
#         self._ingest_rule()

#         self.pipeline = DCASDataPipeline(
#             self.farm_registry_group, self.request_date
#         )
#         self.pipeline.GRID_CROP_NUM_PARTITIONS = 1
#         self.pipeline.data_output.TMP_BASE_DIR = f'/tmp/{uuid.uuid4().hex}'
#         self.pipeline.setup()

#     def tearDown(self):
#         """Clean test resources."""
#         self.pipeline.cleanup()

#     def test_process_grid_crop_data(self):
#         """Test process_grid_crop_data."""
#         grid_data = {
#             'grid_id': [self.grid_1.id, self.grid_2.id],
#             'config_id': [1, 1],
#             'temperature': [10, 12],
#             'humidity': [13, 14],
#             'p_pet': [15, 16],
#             'total_precipitation': [17, 18],
#             'total_evapotranspiration': [19, 20]
#         }
#         for epoch in self.pipeline.data_input.historical_epoch:
#             grid_data[f'max_temperature_{epoch}'] = [21, 22]
#             grid_data[f'min_temperature_{epoch}'] = [23, 24]
#             grid_data[f'total_rainfall_{epoch}'] = [25, 26]

#         grid_df = pd.DataFrame(grid_data)
#         self.pipeline.data_output.save(OutputType.GRID_DATA, grid_df)

#         self.pipeline.process_grid_crop_data()

#         self.assertTrue(
#             os.path.exists(self.pipeline.data_output.grid_crop_data_dir_path)
#         )

#         cassava_id = Crop.objects.get(name='Cassava').id
#         early_id = CropStageType.objects.get(name='Early').id
#         df = read_grid_crop_data(
#             self.pipeline.data_output.grid_crop_data_path,
#             [
#                 f'{cassava_id}_{early_id}_{self.grid_1.id}',
#                 f'{cassava_id}_{early_id}_{self.grid_2.id}'
#             ]
#         )
#         self.assertEqual(df.shape[0], 2)
#         self.assertIn('message', df.columns)
#         self.assertIn('message_2', df.columns)
#         self.assertIn('message_3', df.columns)
#         self.assertIn('message_4', df.columns)
#         self.assertIn('message_5', df.columns)
#         self.assertIn('total_gdd', df.columns)
#         self.assertIn('temperature', df.columns)
#         self.assertIn('p_pet', df.columns)
#         self.assertIn('humidity', df.columns)
#         self.assertIn('seasonal_precipitation', df.columns)
#         self.assertIn('growth_stage_precipitation', df.columns)
#         self.assertIn('grid_id', df.columns)
#         self.assertIn('crop_id', df.columns)
#         self.assertIn('planting_date', df.columns)
#         self.assertIn('growth_stage_id', df.columns)

#         pd.testing.assert_series_equal(
#             df['total_gdd'],
#             pd.Series([60, 66], name='total_gdd', dtype='float64')
#         )
#         pd.testing.assert_series_equal(
#             df['p_pet'],
#             pd.Series([15, 16], name='p_pet', dtype='float64')
#         )
#         pd.testing.assert_series_equal(
#             df['temperature'],
#             pd.Series([10, 12], name='temperature', dtype='float64')
#         )
#         pd.testing.assert_series_equal(
#             df['humidity'],
#             pd.Series([13, 14], name='humidity', dtype='float64')
#         )
#         pd.testing.assert_series_equal(
#             df['seasonal_precipitation'],
#             pd.Series(
#                 [250, 260], name='seasonal_precipitation',
#                 dtype='float64'
#             )
#         )
#         pd.testing.assert_series_equal(
#             df['growth_stage_precipitation'],
#             pd.Series(
#                 [125, 130], name='growth_stage_precipitation',
#                 dtype='float64'
#             )
#         )
