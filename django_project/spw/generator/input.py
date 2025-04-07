# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Data Input Interface.
"""
from datetime import datetime, timezone, timedelta


class SPWDataInput:
    """Class to handle the input data for the SPW generator."""

    variables = [
        'evapotranspirationSum',
        'rainAccumulationSum',
        'temperatureMax',
        'temperatureMin',
        'precipitationProbability',
        'LTNPET',
        'LTNPrecip'
    ]

    columns = [
        'month_day',
        'date',
        'evapotranspirationSum',
        'rainAccumulationSum',
        'LTNPET',
        'LTNPrecip'
    ]

    def __init__(
        self, latitude: float, longitude: float, current_date: datetime
    ) -> None:
        """Initialize the SPWDataInput class."""
        self.data = None
        self.is_data_loaded = False
        self.is_data_valid = False
        self.latitude = latitude
        self.longitude = longitude
        self.current_date = current_date
        self.start_date = current_date - timedelta(days=6)
        self.end_date = current_date + timedelta(days=13)

    def get_data(self):
        """Get the input data."""
        if not self.is_data_loaded:
            self.data = self.load_data()
            self.is_data_loaded = True
            self.validate()
            self.is_data_valid = True
        if not self.is_data_valid:
            raise ValueError("Data is not valid.")
        return self.data

    def load_data(self):
        """Load the input data."""
        raise NotImplementedError("Subclasses must implement this method.")

    def validate(self):
        """Validate the input data."""
        # - data must be a dictionary of month_day and values
        # - month_day must be in %m-%d format
        # - values must be a dictionary with keys matching self.variables
        # - values must be numeric
        # - month_day must be in the range of start_date and end_date

        if not isinstance(self.data, dict):
            raise ValueError("Data must be a dictionary.")
        for month_day, val in self.data.items():
            if not isinstance(month_day, str):
                raise ValueError("month_day must be a string.")
            try:
                date = datetime.strptime(month_day, "%m-%d")
                date = date.replace(
                    year=self.start_date.year,
                    tzinfo=timezone.utc
                )
            except ValueError:
                raise ValueError("month_day must be in %m-%d format.")
            if not (self.start_date <= date <= self.end_date):
                raise ValueError(
                    f"month_day {month_day} is out of range "
                    f"({self.start_date} to {self.end_date})."
                )
            if not isinstance(val, dict):
                raise ValueError("Values must be a dictionary.")
            for key, value in val.items():
                if key not in self.variables + ['date', 'month_day']:
                    raise ValueError(f"Invalid variable: {key}")
                if (
                    key not in ['date', 'month_day'] and
                    not isinstance(value, (int, float))
                ):
                    raise ValueError(
                        f"Value for {key} must be numeric {type(value)}."
                    )

        return True

    def get_spw_data(self):
        """Get data for SPW processing."""
        rows = []
        data = self.get_data()
        for month_day, val in data.items():
            row = [month_day]
            for c in self.columns:
                if c == 'month_day':
                    continue
                row.append(val.get(c, 0))
            rows.append(row)
        return rows
