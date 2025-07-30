# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Dataset Reader.
"""

from config import get_settings
from dataset.base import DatasetType
from dataset.daily_forecast import DailyForecastReader


class DatasetReaderBuilder:
    _readers = {
        DatasetType.DAILY_FORECAST: DailyForecastReader,
        # Add other DatasetType to Reader mappings here
    }

    @classmethod
    def create_reader(cls, dataset_type, *args, **kwargs):
        reader_cls = cls._readers.get(dataset_type)
        if not reader_cls:
            raise ValueError(f"No reader found for dataset type: {dataset_type}")
        
        settings = get_settings()
        if dataset_type == DatasetType.DAILY_FORECAST:
            # Ensure the file_path is set correctly for DailyForecastReader
            args = list(args)           # Convert to list
            args.append(settings.DAILY_FORECAST_ZARR_PATH)    # Append new parameter
            args = tuple(args)
        return reader_cls(*args, **kwargs)
