# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Functions to process row data.
"""

import datetime
import pandas as pd
from sqlalchemy import select, distinct, column, extract, func, cast
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.types import String as SqlString
from geoalchemy2.functions import ST_X, ST_Y, ST_Centroid
import duckdb


class DataQuery:
    """Class to build SQLQuery using sqlalchemy."""

    def __init__(self, limit=None):
        """Initialize query builder."""
        self.base_schema = None
        self.limit = limit
        self.conn_engine = None

    @property
    def grid_id_index_col(self):
        """Get index column for Grid Data Query."""
        return 'gdid'

    @property
    def farmregistry_id_index_col(self):
        """Get index column for FarmRegistry Data Query."""
        return 'farmregistry_id'

    def setup(self, conn_engine):
        """Set the builder class."""
        self.conn_engine = conn_engine
        self._init_schema()

    def _init_schema(self):
        # Use automap base
        self.base_schema = automap_base()

        # Reflect the tables
        self.base_schema.prepare(self.conn_engine, reflect=True)

        # Access reflected tables as classes
        # for table_name, mapped_class in self.base_schema.classes.items():
        #     if table_name != 'gap_farmregistry':
        #         continue
        #     print(f"Table: {table_name}, Class: {mapped_class}")
        #     pprint(vars(mapped_class.__table__))
        #     break

        # all accessed tables here
        self.farmregistry = (
            self.base_schema.classes['gap_farmregistry'].__table__
        )
        self.farm = self.base_schema.classes['gap_farm'].__table__
        self.cropstagetype = (
            self.base_schema.classes['gap_cropstagetype'].__table__
        )
        self.cropgrowthstage = (
            self.base_schema.classes['gap_cropgrowthstage'].__table__
        )
        self.crop = self.base_schema.classes['gap_crop'].__table__
        self.grid = self.base_schema.classes['gap_grid'].__table__
        self.country = self.base_schema.classes['gap_country'].__table__

    def grid_data_query(self, farm_registry_group_ids):
        """Get query for Grid Data."""
        subquery = select(
            self.grid.c.id.label(self.grid_id_index_col),
            self.grid.c.id.label('grid_id'),
            self.country.c.iso_a3.label('iso_a3'),
            self.country.c.id.label('country_id'),
            ST_Centroid(self.grid.c.geometry).label('centroid')
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).join(
            self.country, self.grid.c.country_id == self.country.c.id
        ).where(
            self.farmregistry.c.group_id.in_(farm_registry_group_ids)
        ).order_by(
            self.grid.c.id
        )

        if self.limit:
            # for testing purpose
            subquery = subquery.limit(self.limit)

        subquery = subquery.subquery('grid_data')
        return select(
            distinct(column(self.grid_id_index_col)),
            ST_Y(column('centroid')).label('lat'),
            ST_X(column('centroid')).label('lon'),
            column('grid_id'),
            column('iso_a3'),
            column('country_id'),
        ).select_from(subquery)

    def _grid_data_with_crop_subquery(self, farm_registry_group_ids):
        return select(
            self.grid.c.id.label(self.grid_id_index_col),
            self.grid.c.id.label('grid_id'),
            self.farmregistry.c.crop_id,
            self.farmregistry.c.crop_stage_type_id,
            self.farmregistry.c.planting_date,
            extract(
                'epoch',
                func.DATE(self.farmregistry.c.planting_date)
            ).label('planting_date_epoch'),
            self.farmregistry.c.crop_growth_stage_id.label(
                'prev_growth_stage_id'
            ),
            extract(
                'epoch',
                func.DATE(self.farmregistry.c.growth_stage_start_date)
            ).label('prev_growth_stage_start_date'),
            (
                cast(self.farmregistry.c.crop_id, SqlString) + '_' +
                cast(self.farmregistry.c.crop_stage_type_id, SqlString) + '_' +
                cast(self.grid.c.id, SqlString)
            ).label('grid_crop_key')
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).where(
            self.farmregistry.c.group_id.in_(farm_registry_group_ids)
        ).order_by(
            self.grid.c.id
        )

    def grid_data_with_crop_query(self, farm_registry_group_ids):
        """Get grid data with crop query."""
        subquery = self._grid_data_with_crop_subquery(farm_registry_group_ids)
        if self.limit:
            # for testing purpose
            subquery = subquery.limit(self.limit)

        subquery = subquery.subquery('grid_data')
        return select(
            column(self.grid_id_index_col), column('crop_id'),
            column('crop_stage_type_id'), column('planting_date'),
            column('prev_growth_stage_id'),
            column('prev_growth_stage_start_date'),
            column('grid_id'),
            column('planting_date_epoch'),
            column('grid_crop_key')
        ).distinct().select_from(subquery)

    def grid_data_with_crop_meta(self, farm_registry_group_ids):
        """Get metadata for grid with crop data."""
        subquery = self._grid_data_with_crop_subquery(farm_registry_group_ids)
        subquery = subquery.limit(1)
        subquery = subquery.subquery('grid_data')
        sql_query = select(
            column(self.grid_id_index_col),
            column('crop_id'),
            column('crop_stage_type_id'), column('planting_date'),
            column('prev_growth_stage_id'),
            column('prev_growth_stage_start_date'),
            column('grid_id'), column('planting_date_epoch'),
            column('grid_crop_key')
        ).distinct().select_from(subquery)
        with self.conn_engine.connect() as conn:
            df = pd.read_sql_query(
                sql_query,
                con=conn,
                index_col=self.grid_id_index_col,
            )
        df['prev_growth_stage_id'] = (
            df['prev_growth_stage_id'].astype('Int64')
        )
        df['prev_growth_stage_start_date'] = (
            df['prev_growth_stage_start_date'].astype('Float64')
        )
        return df

    def _farm_registry_subquery(self, farm_registry_group_ids):
        subquery = select(
            self.farmregistry.c.id.label('farmregistry_id'),
            self.farmregistry.c.planting_date.label('planting_date'),
            extract(
                'epoch',
                func.DATE(self.farmregistry.c.planting_date)
            ).label('planting_date_epoch'),
            self.farmregistry.c.crop_id.label('crop_id'),
            self.farmregistry.c.crop_stage_type_id.label(
                'crop_stage_type_id'
            ),
            self.farmregistry.c.group_id,
            self.farm.c.id.label('farm_id'),
            self.farm.c.unique_id.label('farm_unique_id'),
            self.farm.c.geometry.label('geometry'),
            self.grid.c.id.label('grid_id'),
            self.grid.c.unique_id.label('grid_unique_id'),
            self.farmregistry.c.id.label('registry_id'),
            (self.crop.c.name + '_' + self.cropstagetype.c.name).label('crop'),
            self.country.c.iso_a3.label('iso_a3'),
            self.country.c.id.label('country_id'),
            (
                cast(self.crop.c.id, SqlString) + '_' +
                cast(self.cropstagetype.c.id, SqlString) + '_' +
                cast(self.grid.c.id, SqlString)
            ).label('grid_crop_key'),
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).join(
            self.crop, self.farmregistry.c.crop_id == self.crop.c.id
        ).join(
            self.cropstagetype,
            self.farmregistry.c.crop_stage_type_id == self.cropstagetype.c.id
        ).join(
            self.country, self.grid.c.country_id == self.country.c.id
        ).where(
            self.farmregistry.c.group_id.in_(farm_registry_group_ids)
        ).order_by(
            self.grid.c.id, self.farmregistry.c.id
        )

        return subquery

    def farm_registry_query(self, farm_registry_group_ids):
        """Get Farm Registry data query."""
        subquery = self._farm_registry_subquery(farm_registry_group_ids)
        if self.limit:
            # for testing purpose
            subquery = subquery.limit(self.limit)

        subquery = subquery.subquery('farm_data')

        return select(subquery)

    def farm_registry_meta(self, farm_registry_group_ids, request_date):
        """Get metadata for farm registry query."""
        subquery = self._farm_registry_subquery(farm_registry_group_ids)
        subquery = subquery.limit(1)
        subquery = subquery.subquery('farm_data')

        sql_query = select(subquery)
        with self.conn_engine.connect() as conn:
            df = pd.read_sql_query(
                sql_query,
                con=conn,
                index_col=self.farmregistry_id_index_col,
            )

        df = df.assign(
            date=pd.Timestamp(request_date),
            year=lambda x: x.date.dt.year,
            month=lambda x: x.date.dt.month,
            day=lambda x: x.date.dt.day
        )
        return df

    def read_grid_data_crop_meta_parquet(
        self, parquet_file_path
    ) -> pd.DataFrame:
        """Read grid data from parquet file.

        :param parquet_file_path: file_path to parquet file
        :type parquet_file_path: str
        :return: DataFrame
        :rtype: pd.DataFrame
        """
        if not parquet_file_path.endswith('.parquet'):
            parquet_file_path += '/*.parquet'
        conndb = duckdb.connect()
        query = (
            f"""
            SELECT *
            FROM read_parquet('{parquet_file_path}')
            LIMIT 1
            """
        )
        df = conndb.sql(query).df()
        conndb.close()
        return df

    def get_farms_without_messages(
        date: datetime.date, parquet_path: str, conn, chunk_size: int = 500
    ):
        """
        Fetch farms without advisory messages using chunked processing.

        :param date: FarmRegistries date to be filtered.
        :type date: datetime.date
        :param parquet_path: Path to the final Parquet file.
        :type parquet_path: str
        :param conn: DuckDB connection.
        :type conn: DuckDB connection
        :param chunk_size: Number of records per chunk (default: 500).
        :type chunk_size: int
        :return: Generator yielding Pandas DataFrames in chunks.
        :rtype: Generator[pd.DataFrame]
        """
        offset = 0  # Start at the beginning

        try:
            while True:
                query = f"""
                    SELECT farm_id, crop, farm_unique_id, growth_stage
                    FROM read_parquet('{parquet_path}', hive_partitioning=true)
                    WHERE message IS NULL
                    AND message_2 IS NULL
                    AND message_3 IS NULL
                    AND message_4 IS NULL
                    AND message_5 IS NULL
                    AND year={date.year} AND month={date.month} AND
                    day={date.day}
                    ORDER BY registry_id
                    LIMIT {chunk_size} OFFSET {offset}
                """
                df = conn.sql(query).df()

                if df.empty:
                    break  # Stop when there are no more records

                yield df  # Yield the chunk
                offset += chunk_size  # Move to the next batch

        except Exception as e:
            print(f"Error querying Parquet: {str(e)}")
        finally:
            conn.close()
