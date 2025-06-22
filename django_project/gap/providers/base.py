# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Builder base class.
"""

from typing import List
from datetime import datetime

from gap.models import Dataset, DatasetAttribute
from gap.utils.reader import BaseDatasetReader, DatasetReaderInput


class BaseReaderBuilder:
    """Base class for Dataset Reader Builder."""

    def __init__(
        self, dataset: Dataset, attributes: List[DatasetAttribute],
        location_input: DatasetReaderInput,
        start_date: datetime, end_date: datetime,
        use_cache: bool = True
    ):
        """Initialize BaseReaderBuilder class."""
        self.dataset = dataset
        self.attributes = attributes
        self.location_input = location_input
        self.start_date = start_date
        self.end_date = end_date
        # Disable cache until we implement caching logic
        # without race conditions
        self.use_cache = False

    def build(self) -> BaseDatasetReader:
        """Build a new Dataset Reader."""
        # TODO: we might need to refactor setup_reader in this method
        raise NotImplementedError
