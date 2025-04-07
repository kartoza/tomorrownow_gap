from datetime import datetime
from django.contrib.gis.geos import Point

from gap.models import Dataset, DatasetAttribute, CastType, DatasetStore
from gap.providers import TomorrowIODatasetReader, TIO_PROVIDER
from gap.utils.reader import DatasetReaderInput
from .input import SPWDataInput


class GapInput(SPWDataInput):
    """Class to handle the input data for the SPW generator."""

    # Attributes in GAP table
    ATTRIBUTES = [
        'total_evapotranspiration_flux',
        'total_rainfall',
        'max_temperature',
        'min_temperature',
        'precipitation_probability'
    ]

    VAR_MAPPING = {
        'total_evapotranspiration_flux': 'evapotranspirationSum',
        'total_rainfall': 'rainAccumulationSum',
        'max_temperature': 'temperatureMax',
        'min_temperature': 'temperatureMin',
        'precipitation_probability': 'precipitationProbability',
    }

    LTN_MAPPING = {
        'total_evapotranspiration_flux': 'LTNPET',
        'total_rainfall': 'LTNPrecip'
    }

    def __init__(
        self, latitude: float, longitude: float, current_date: datetime
    ) -> None:
        """Initialize the SPWDataInput class."""
        super().__init__(latitude, longitude, current_date)
        self.location_input = DatasetReaderInput.from_point(
            Point(longitude, latitude),
        )

    def _fetch_timelines_data_dataset(self):
        """Return dataset that will be used for _fetch_timelines_data."""
        return Dataset.objects.filter(
            provider__name=TIO_PROVIDER,
            type__type=CastType.HISTORICAL
        ).exclude(
            type__name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
        ).first()

    def calculate_from_point_attrs(self):
        """Return attributes that are being used in calculate from point."""
        return DatasetAttribute.objects.filter(
            attribute__variable_name__in=self.ATTRIBUTES,
            dataset__provider__name=TIO_PROVIDER,
            dataset__store_type=DatasetStore.EXT_API
        )

    def _fetch_timelines_data(self) -> dict:
        """Fetch historical and forecast data for given location.

        :param location_input: Location for the query
        :type location_input: DatasetReaderInput
        :param attrs: List of attributes
        :type attrs: List[DatasetAttribute]
        :param start_dt: Start date time
        :type start_dt: datetime
        :param end_dt: End date time
        :type end_dt: datetime
        :return: Dictionary of month_day and results
        :rtype: dict
        """
        attrs = self.calculate_from_point_attrs()
        historical_attrs = list(
            attrs.filter(dataset__type__type=CastType.FORECAST)
        )
        dataset = self._fetch_timelines_data_dataset()
        reader = TomorrowIODatasetReader(
            dataset, historical_attrs, self.location_input,
            self.start_date, self.end_date)
        reader.read()
        # if not reader.is_success():
        #     raise Exception(
        #         f'Failed to fetch Tomorrow.io API! {str(reader.errors)}'
        #     )
        results = {}
        for val in reader.get_raw_results():
            month_day = val.get_datetime_repr('%m-%d')
            val_dict = val.to_dict()['values']
            data = {
                'date': val.get_datetime_repr('%Y-%m-%d')
            }
            for k, v in self.VAR_MAPPING.items():
                data[v] = val_dict.get(k, 0)
            results[month_day] = data
        return results

    def _fetch_ltn_data(self, historical_dict: dict) -> dict:
        """Fetch Long Term Normals data for given location.

        The resulting data will be merged into historical_dict.

        :param location_input: Location for the query
        :type location_input: DatasetReaderInput
        :param attrs: List of attributes
        :type attrs: List[DatasetAttribute]
        :param start_dt: Start date time
        :type start_dt: datetime
        :param end_dt: End date time
        :type end_dt: datetime
        :param historical_dict: Dictionary from historical data
        :type historical_dict: dict
        :return: Merged dictinoary with LTN data
        :rtype: dict
        """
        dataset = Dataset.objects.filter(
            provider__name=TIO_PROVIDER,
            type__type=CastType.HISTORICAL,
            type__name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
        ).first()
        attrs = self.calculate_from_point_attrs()
        ltn_attrs = list(
            attrs.filter(
                dataset__type__name=
                TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
            )
        )
        reader = TomorrowIODatasetReader(
            dataset, ltn_attrs, self.location_input,
            self.start_date, self.end_date
        )
        reader.read()
        # if not reader.is_success():
        #     raise Exception(
        #         f'Failed to fetch Tomorrow.io API! {str(reader.errors)}'
        #     )
        for val in reader.get_raw_results():
            month_day = val.get_datetime_repr('%m-%d')
            if month_day in historical_dict:
                data = historical_dict[month_day]
                for k, v in self.LTN_MAPPING.items():
                    data[v] = val.values.get(k, '')
        return historical_dict

    def load_data(self):
        """Load the input data."""
        historical_dict = self._fetch_timelines_data()
        final_dict = self._fetch_ltn_data(historical_dict)
        return final_dict
