# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from gap.ingestor.wind_borne_systems import PROVIDER as WINBORNE_PROVIDER
from gap.models import Dataset, DatasetStore
from gap.providers.airborne_observation import ObservationAirborneDatasetReader
from gap.providers.cbam import CBAMZarrReader, CBAMNetCDFReader  # noqa
from gap.providers.observation import (
    ObservationDatasetReader, TahmoParquetReader
)
from gap.providers.salient import (
    SalientNetCDFReader, SalientZarrReader
)  # noqa
from gap.providers.tio import (
    TomorrowIODatasetReader,
    PROVIDER_NAME as TIO_PROVIDER,
    TioZarrReader
)
from gap.utils.netcdf import NetCDFProvider


def get_reader_from_dataset(dataset: Dataset):
    """Create a new Reader from given dataset.

    :param dataset: Dataset to be read
    :type dataset: Dataset
    :raises TypeError: if provider is neither CBAM or Salient
    :return: Reader Class Type
    :rtype: BaseDatasetReader
    """
    if dataset.provider.name == NetCDFProvider.CBAM:
        return CBAMZarrReader
    elif dataset.provider.name == NetCDFProvider.SALIENT:
        return SalientZarrReader
    # elif dataset.name == 'Tahmo Ground Observational':
    #     return TahmoParquetReader
    elif dataset.provider.name in ['Tahmo', 'Arable']:
        return ObservationDatasetReader
    elif dataset.provider.name in [WINBORNE_PROVIDER]:
        return ObservationAirborneDatasetReader
    elif (
        dataset.provider.name == TIO_PROVIDER and
        dataset.store_type == DatasetStore.EXT_API
    ):
        return TomorrowIODatasetReader
    elif (
        dataset.provider.name == TIO_PROVIDER and
        dataset.store_type == DatasetStore.ZARR
    ):
        return TioZarrReader
    else:
        raise TypeError(
            f'Unsupported provider name: {dataset.provider.name}'
        )
