# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tamsat SPW Reader
"""

import duckdb

from spw.tamsat.base import TamsatSPWBase


class TamsatSPWReader(TamsatSPWBase):
    """Reader for TAMSAT SPW data."""

    def __init__(self, verbose=False):
        """Initialize the TamsatSPWReader."""
        super().__init__(verbose=verbose)
        self.dates = []
        self.conn = None

    def setup(self):
        """Set up the TamsatSPWReader."""
        self._init_config()
        config = self._get_duckdb_config()
        self.conn = duckdb.connect(config=config)
        self.conn.install_extension("httpfs")
        self.conn.load_extension("httpfs")
        self.conn.install_extension("spatial")
        self.conn.load_extension("spatial")
        self.conn.execute(self.CREATE_TABLE_QUERY.format(self.SPW_TABLE_NAME))
        self.dates = []

    def read_data(self, date, farmer_ids=None, farm_groups=None):
        """Read SPW data for a specific date."""
        if not self.conn:
            raise ValueError(
                "Connection not established. Call setup() first."
            )

        if not self._check_parquet_exists(date):
            raise ValueError(
                f"No data available for date: {date}. "
                "Ensure the data has been generated."
            )

        first_day_in_month = date.replace(day=1)
        if first_day_in_month not in self.dates:
            self._pull_existing_monthly_data(self.conn, first_day_in_month)
            self.dates.append(first_day_in_month)

        query = f"""
            SELECT * FROM {self.SPW_TABLE_NAME}
            WHERE date = $1
        """
        param_id = 2
        params = [date]
        if farmer_ids:
            query += (
                f" AND farm_unique_id IN ${param_id}"
            )
            param_id += 1
            params.append(farmer_ids)
        if farm_groups:
            query += f" AND farm_group IN ${param_id}"
            param_id += 1
            params.append(farm_groups)
        return self.conn.execute(query, parameters=params).df()

    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
