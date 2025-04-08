# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Data Input using GAP.
"""
from datetime import datetime
import numpy as np
import pytz
from django.contrib.gis.geos import Point
from django.core.cache import cache

from gap.models import (
    Dataset, DatasetAttribute, CastType,
    DatasetStore, DataSourceFile
)
from gap.providers import (
    TomorrowIODatasetReader, TIO_PROVIDER,
    TioZarrReader
)
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

    # Cache key for checking Tio zarr
    CACHE_KEY_TIO_ZARR = 'forecast_tio_zarr_available'
    # Check every 10 minutes
    CHECK_CACHE_KEY_TIO_ZARR_EXPIRY = 60 * 10  # 10 minutes
    CACHE_KEY_TIO_ZARR_EXPIRY = 60 * 60 * 2  # 2 hours

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
                TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE,
                attribute__variable_name__in=self.LTN_MAPPING.keys()
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

    def _is_date_in_zarr(self, date: datetime) -> bool:
        cached_value = cache.get(self.CACHE_KEY_TIO_ZARR)
        if cached_value is not None:
            return cached_value

        # check from zarr and store to cache
        result = False
        dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        reader = TioZarrReader(
            dataset,
            [],
            self.location_input,
            self.start_date,
            self.end_date
        )
        reader.setup_reader()
        zarr_file = DataSourceFile.objects.filter(
            dataset=dataset,
            format=DatasetStore.ZARR,
            is_latest=True
        ).order_by('id').last()
        if zarr_file is not None:
            ds = reader.open_dataset(zarr_file)
            existing_dates = ds['forecast_date'].values
            np_date = np.datetime64(f'{date.date().isoformat()}')
            result = np_date in existing_dates
            ds.close()

        # store result to cache
        cache.set(
            self.CACHE_KEY_TIO_ZARR,
            result,
            self.CACHE_KEY_TIO_ZARR_EXPIRY if result else
            self.CHECK_CACHE_KEY_TIO_ZARR_EXPIRY
        )
        return result

    def _read_forecast_from_zarr(self) -> dict:
        """Read forecast data from T.io zarr."""
        dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        attributes = DatasetAttribute.objects.filter(
            attribute__variable_name__in=self.ATTRIBUTES,
            dataset=dataset
        )
        reader = TioZarrReader(
            dataset,
            list(attributes),
            self.location_input,
            self.start_date,
            self.end_date
        )
        reader.read()
        reader_value = reader.get_data_values()
        results = {}
        for dt_idx, dt in enumerate(
            reader_value.xr_dataset['date'].values):
            timestamp = (
                    (dt - np.datetime64('1970-01-01T00:00:00')) /
                    np.timedelta64(1, 's')
            )
            dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
            month_day = dt.strftime('%m-%d')
            data = {
                'date': dt.strftime('%Y-%m-%d')
            }
            for var_name in self.ATTRIBUTES:
                v = reader_value.xr_dataset[var_name].values[dt_idx]
                var_map = self.VAR_MAPPING.get(var_name)
                data[var_map] = (
                    v if not np.isnan(v) else None
                )
            results[month_day] = data

        return results

    def clear_cache(self):
        """Clear the cache for Tio zarr."""
        cache.delete(self.CACHE_KEY_TIO_ZARR)

    def load_data(self):
        """Load the input data."""
        # Check if the date is in zarr
        is_date_in_zarr = self._is_date_in_zarr(self.current_date)
        if is_date_in_zarr:
            historical_dict = self._read_forecast_from_zarr()
        else:
            historical_dict = self._fetch_timelines_data()

        final_dict = self._fetch_ltn_data(historical_dict)
        return final_dict
