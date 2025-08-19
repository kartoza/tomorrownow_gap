# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from typing import List
from datetime import datetime

from gap.ingestor.wind_borne_systems import PROVIDER as WINBORNE_PROVIDER
from gap.models import Dataset, DatasetStore, DatasetAttribute
from gap.utils.reader import DatasetReaderInput
from gap.providers.base import BaseReaderBuilder
from gap.providers.airborne_observation import (
    ObservationAirborneDatasetReader,
    ObservationAirborneParquetReader,
    ObservationAirborneReaderBuilder
)  # noqa
from gap.providers.cbam import (
    CBAMZarrReader, CBAMNetCDFReader,
    CBAMReaderBuilder,
    CBAMHourlyForecastHistoricalReader
)  # noqa
from gap.providers.observation import (
    ObservationDatasetReader, ObservationParquetReader,
    ObservationReaderBuilder
)  # noqa
from gap.providers.salient import (
    SalientNetCDFReader, SalientZarrReader,
    SalientReaderBuilder
)  # noqa
from gap.providers.tio import (
    TomorrowIODatasetReader,
    PROVIDER_NAME as TIO_PROVIDER,
    TioZarrReader,
    TioReaderBuilder
)  # noqa
from gap.providers.tamsat import (
    TamsatReaderBuilder,
    TamsatZarrReader,
    PROVIDER_NAME as TAMSAT_PROVIDER
)  # noqa
from gap.providers.google import (
    GoogleReaderBuilder,
    GoogleNowcastZarrReader,
    PROVIDER_NAME as GOOGLE_PROVIDER
)  # noqa
from gap.utils.netcdf import NetCDFProvider


def get_reader_builder(
    dataset: Dataset, attributes: List[DatasetAttribute],
    location_input: DatasetReaderInput,
    start_date: datetime, end_date: datetime, **kwargs
) -> BaseReaderBuilder:
    """Create a new Reader Builder from given dataset.

    :param dataset: Dataset to be read
    :type dataset: Dataset
    :raises TypeError: if provider is unsupported
    :return: Reader Builder
    :rtype: BaseReaderBuilder
    """
    if dataset.provider.name == NetCDFProvider.CBAM:
        return CBAMReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date
        )
    elif dataset.provider.name == NetCDFProvider.SALIENT:
        forecast_date = kwargs.get('forecast_date', None)
        return SalientReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date,
            forecast_date=forecast_date
        )
    elif dataset.provider.name in ['Tahmo', 'Arable']:
        use_parquet = kwargs.get('use_parquet', False)
        return ObservationReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date, use_parquet=use_parquet
        )
    elif dataset.provider.name in [WINBORNE_PROVIDER]:
        use_parquet = kwargs.get('use_parquet', False)
        altitudes = kwargs.get('altitudes', None)
        return ObservationAirborneReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date,
            altitudes=altitudes,
            use_parquet=use_parquet
        )
    elif dataset.provider.name == TIO_PROVIDER:
        return TioReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date
        )
    elif dataset.provider.name == TAMSAT_PROVIDER:
        return TamsatReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date
        )
    elif dataset.provider.name == GOOGLE_PROVIDER:
        return GoogleReaderBuilder(
            dataset, attributes, location_input,
            start_date, end_date
        )
    else:
        raise TypeError(
            f'Unsupported provider name: {dataset.provider.name}'
        )
