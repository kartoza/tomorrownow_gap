# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Data Pipeline
"""

import logging
import datetime
import time
import numpy as np
import pandas as pd
from django.core.cache import cache
from django.db import connection
from django.db.models import Min
import dask.dataframe as dd
from dask.dataframe.core import DataFrame as dask_df
from django.contrib.gis.db.models import Union
from dcas.service import GrowthStageService
from gap.models import FarmRegistryGroup, FarmRegistry, Grid, CropGrowthStage
from dcas.models import DCASConfig, DCASConfigCountry
from dcas.partitions import (
    process_partition_total_gdd,
    process_partition_growth_stage,
    process_partition_growth_stage_precipitation,
    process_partition_message_output,
    process_partition_other_params,
    process_partition_seasonal_precipitation,
    process_partition_farm_registry
)
from dcas.queries import DataQuery
from dcas.outputs import DCASPipelineOutput, OutputType
from dcas.inputs import DCASPipelineInput


logger = logging.getLogger(__name__)
# Enable copy_on_write CoW globally
pd.set_option("mode.copy_on_write", True)


class DCASDataPipeline:
    """Class for DCAS data pipeline."""

    NUM_PARTITIONS = 10
    # GRID_CROP_NUM_PARTITIONS = 100
    GRID_CROP_NUM_PARTITIONS = 2
    LIMIT = 10000

    def __init__(
        self, farm_registry_group: FarmRegistryGroup,
        request_date: datetime.date
    ):
        """Initialize DCAS Data Pipeline.

        :param farm_registry_group: _description_
        :type farm_registry_group: FarmRegistryGroup
        """
        self.farm_registry_group = farm_registry_group
        self.fs = None
        self.minimum_plant_date = None
        self.crops = []
        self.request_date = request_date
        self.data_query = DataQuery(self._conn_str(), self.LIMIT)
        self.data_output = DCASPipelineOutput(request_date)
        self.data_input = DCASPipelineInput(request_date)

    def setup(self):
        """Set the data pipeline."""
        self.data_query.setup()

        # fetch minimum plant date
        self.minimum_plant_date: datetime.date = FarmRegistry.objects.filter(
            group=self.farm_registry_group
        ).aggregate(Min('planting_date'))['planting_date__min']
        # fetch crop id list
        farm_qs = FarmRegistry.objects.filter(
            group=self.farm_registry_group
        ).order_by('crop_id').values_list(
            'crop_id', flat=True
        ).distinct('crop_id')
        self.crops = list(farm_qs)

        # initialize output
        self.data_output.setup()

        # initialize input
        self.data_input.setup(self.minimum_plant_date)

    def _conn_str(self):
        return 'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}'.format(
            **connection.settings_dict
        )

    def cleanup_gdd_matrix(self):
        """Cleanup GDD Matrix."""
        GrowthStageService.cleanup_matrix()

    def load_grid_data(self) -> pd.DataFrame:
        """Load grid data from FarmRegistry table.

        :return: DataFrame of Grid Data
        :rtype: pd.DataFrame
        """
        df = pd.read_sql_query(
            self.data_query.grid_data_query(self.farm_registry_group),
            con=self._conn_str(),
            index_col=self.data_query.grid_id_index_col,
        )

        return self._merge_grid_data_with_config(df)

    def _merge_grid_data_with_config(self, df: pd.DataFrame) -> pd.DataFrame:
        default_config = DCASConfig.objects.filter(
            is_default=True
        ).first()
        config_map = {}
        countries = Grid.objects.filter(
            id__in=df.index.unique()
        ).distinct(
            'country_id'
        ).order_by('country_id').values_list(
            'country_id',
            flat=True
        )
        for country_id in countries:
            if country_id is None:
                continue
            config_for_country = DCASConfigCountry.objects.filter(
                country_id=country_id
            ).first()
            if config_for_country:
                config_map[country_id] = config_for_country.config.id

        if config_map:
            df['config_id'] = df['country_id'].map(
                config_map
            ).fillna(default_config.id)
        else:
            df['config_id'] = default_config.id

        df['config_id'] = df['config_id'].astype('Int64')

        return df

    def load_grid_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Load Grid Weather data.

        TODO: replace with function to read from ZARR/NetCDF File.
        :param df: DataFrame of Grid Data
        :type df: pd.DataFrame
        :return: Grid DataFrame with weather columns
        :rtype: pd.DataFrame
        """
        # Combine all geometries and compute the bounding box
        combined_bbox = (
            Grid.objects.filter(
                id__in=df.index.unique()
            ).aggregate(
                combined_geometry=Union('geometry')
            )
        )

        bbox = combined_bbox['combined_geometry'].extent
        print(f"Bounding Box: {bbox}")

        return self.data_input.collect_data(bbox, df)

    def postprocess_grid_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate value for Grid parameters.

        :param df: Grid DataFrame
        :type df: pd.DataFrame
        :return: DataFrame with column: temperature, humidity,
            total_precipitation, total_evapotranspiration, p_pet
        :rtype: pd.DataFrame
        """
        base_cols = []
        temp_cols = []
        humidity_cols = []
        dates_humidity = pd.date_range(
            self.request_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )

        for date in dates_humidity:
            epoch = int(date.timestamp())
            daily_avg_temp = f'daily_avg_temp_{epoch}'
            temp_cols.append(daily_avg_temp)
            df[daily_avg_temp] = (
                df[f'max_temperature_{epoch}'] +
                df[f'min_temperature_{epoch}']
            ) / 2

            daily_avg_humidity = f'daily_avg_humidity_{epoch}'
            humidity_cols.append(daily_avg_humidity)
            df[daily_avg_humidity] = (
                df[f'max_humidity_{epoch}'] + df[f'min_humidity_{epoch}']
            ) / 2

            base_cols.extend([
                f'max_humidity_{epoch}',
                f'min_humidity_{epoch}'
            ])

            # to be confirmed: GDD only from plantingDate to today date?
            # or through 3rd forecast day?
            # if date > self.request_date:
            #     base_cols.extend([
            #         f'max_temperature_{epoch}',
            #         f'min_temperature_{epoch}'
            #     ])

        df['temperature'] = df[temp_cols].mean(axis=1)
        df['humidity'] = df[humidity_cols].mean(axis=1)

        precip_cols = []
        evap_cols = []
        dates_precip = pd.date_range(
            self.request_date - datetime.timedelta(days=6),
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates_precip:
            epoch = int(date.timestamp())
            precip_cols.append(f'precipitation_{epoch}')
            evap_cols.append(f'evapotranspiration_{epoch}')
            base_cols.extend([
                f'precipitation_{epoch}',
                f'evapotranspiration_{epoch}'
            ])
        df['total_precipitation'] = df[precip_cols].sum(axis=1)
        df['total_evapotranspiration'] = df[evap_cols].sum(axis=1)
        df['p_pet'] = (
            df['total_precipitation'] / df['total_evapotranspiration']
        )

        df = df.drop(columns=base_cols + temp_cols + humidity_cols)
        return df

    def load_grid_data_with_crop(self) -> dask_df:
        """Load Grid Data distinct with crop and crop_stage_type.

        :return: Dask DataFrame
        :rtype: dask_df
        """
        ddf = dd.read_sql_query(
            sql=self.data_query.grid_data_with_crop_query(
                self.farm_registry_group
            ),
            con=self._conn_str(),
            index_col=self.data_query.grid_id_index_col,
            npartitions=self.GRID_CROP_NUM_PARTITIONS,
        )

        # adjust type
        ddf = ddf.astype({
            'prev_growth_stage_id': 'Int64',
            'prev_growth_stage_start_date': 'Float64'
        })

        return ddf

    def load_farm_registry_data(self) -> dask_df:
        """Load Farm Registry Data.

        :return: Dask DataFrame
        :rtype: dask_df
        """
        sql_query = self.data_query.farm_registry_query(
            self.farm_registry_group
        )

        df = dd.read_sql_query(
            sql=sql_query,
            con=self._conn_str(),
            index_col=self.data_query.farmregistry_id_index_col,
            npartitions=self.NUM_PARTITIONS,
        )

        df = df.assign(
            date=pd.Timestamp(self.request_date),
            year=lambda x: x.date.dt.year,
            month=lambda x: x.date.dt.month,
            day=lambda x: x.date.dt.day
        )
        return df

    def data_collection(self):
        """Run Data Collection step."""
        grid_df = self.load_grid_data()

        grid_df = self.load_grid_weather_data(grid_df)

        grid_df = self.postprocess_grid_weather_data(grid_df)

        self.data_output.save(OutputType.GRID_DATA, grid_df)
        del grid_df

    def process_grid_crop_data(self):
        """Process Grid and Crop Data."""
        # Load GDD Matrix before processing grid crop data
        GrowthStageService.load_matrix()

        grid_data_file_path = self.data_output.grid_data_file_path

        # load grid with crop and planting date
        grid_crop_df = self.load_grid_data_with_crop()
        grid_crop_df_meta = self.data_query.grid_data_with_crop_meta(
            self.farm_registry_group
        )

        # Process gdd cumulative
        # add config_id
        grid_crop_df_meta = grid_crop_df_meta.assign(
            config_id=pd.Series(dtype='Int64')
        )
        gdd_columns = []
        for epoch in self.data_input.historical_epoch:
            grid_crop_df_meta[f'gdd_sum_{epoch}'] = np.nan
            gdd_columns.append(f'gdd_sum_{epoch}')

        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_total_gdd,
            grid_data_file_path,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # Identify crop growth stage
        grid_crop_df_meta = grid_crop_df_meta.assign(
            growth_stage_start_date=pd.Series(dtype='double'),
            growth_stage_id=pd.Series(dtype='int'),
            total_gdd=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_growth_stage,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # drop gdd columns
        grid_crop_df = grid_crop_df.drop(columns=gdd_columns)
        grid_crop_df_meta = grid_crop_df_meta.drop(columns=gdd_columns)

        # Process seasonal_precipitation
        grid_crop_df_meta = grid_crop_df_meta.assign(
            seasonal_precipitation=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_seasonal_precipitation,
            grid_data_file_path,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # Add temperature, humidity, and p_pet
        grid_crop_df_meta = grid_crop_df_meta.assign(
            temperature=np.nan,
            humidity=np.nan,
            p_pet=np.nan,
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_other_params,
            grid_data_file_path,
            meta=grid_crop_df_meta
        )

        # Calculate growth_stage_precipitation
        grid_crop_df_meta = grid_crop_df_meta.assign(
            growth_stage_precipitation=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_growth_stage_precipitation,
            grid_data_file_path,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # Calculate message codes
        grid_crop_df_meta = grid_crop_df_meta.assign(
            message=None,
            message_2=None,
            message_3=None,
            message_4=None,
            message_5=None
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_message_output,
            meta=grid_crop_df_meta
        )

        self.data_output.save(OutputType.GRID_CROP_DATA, grid_crop_df)

    def process_farm_registry_data(self):
        """Merge with farm registry data."""
        farm_df = self.load_farm_registry_data()
        farm_df_meta = self.data_query.farm_registry_meta(
            self.farm_registry_group, self.request_date
        )

        # merge with grid crop data meta
        farm_df_meta = self._append_grid_crop_meta(farm_df_meta)

        # load mapping for CropGrowthStage
        growth_stage_mapping = {}
        for growth_stage in CropGrowthStage.objects.all():
            growth_stage_mapping[growth_stage.id] = growth_stage.name

        farm_df = farm_df.map_partitions(
            process_partition_farm_registry,
            self.data_output.grid_crop_data_path,
            growth_stage_mapping,
            meta=farm_df_meta
        )

        self.data_output.save(OutputType.FARM_CROP_DATA, farm_df)

    def _append_grid_crop_meta(self, farm_df_meta: pd.DataFrame):
        # load from grid_crop data
        grid_crop_df_meta = self.data_query.read_grid_data_crop_meta_parquet(
            self.data_output.grid_crop_data_dir_path
        )

        # adding new columns:
        # - prev_growth_stage_id, prev_growth_stage_start_date,
        # - config_id, growth_stage_start_date, growth_stage_id,
        # - total_gdd, seasonal_precipitation, temperature, humidity,
        # - p_pet, growth_stage_precipitation
        # - message, message_2, message_3, message_4, message_5
        # - growth_stage
        meta = grid_crop_df_meta.drop(columns=[
            'crop_id', 'crop_stage_type_id', 'planting_date',
            'grid_id', 'planting_date_epoch', '__null_dask_index__'
        ])
        # add growth_stage
        meta = meta.assign(growth_stage=None)
        return pd.concat([farm_df_meta, meta], axis=1)

    def extract_csv_output(self):
        """Extract csv output file."""
        file_path = self.data_output.convert_to_csv()

        return file_path

    def run(self):
        """Run data pipeline."""
        self.setup()

        start_time = time.time()
        self.data_collection()
        self.process_grid_crop_data()

        self.process_farm_registry_data()
        self.extract_csv_output()

        self.cleanup_gdd_matrix()

        print(f'Finished {time.time() - start_time} seconds.')
