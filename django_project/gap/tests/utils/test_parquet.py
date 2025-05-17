# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Parquet test.
"""

import datetime
import mock
from django.test import TestCase, override_settings
from django.core.files.storage import storages
from django.core.files.uploadedfile import SimpleUploadedFile

from core.settings.utils import absolute_path
from gap.models import (
    Dataset, IngestorType, IngestorSession, DataSourceFile,
    DatasetStore
)
from gap.utils.parquet import (
    ParquetConverter,
    ParquetIngestorAppender,
    WindborneParquetIngestorAppender
)
from gap.tasks.ingestor import convert_dataset_to_parquet


class ParquetConverterTest(TestCase):
    """Class for ParquetConverter test."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.TAHMO

    def setUp(self):
        """Initialize ParquetConverter class."""
        self.dataset = Dataset.objects.get(
            name='Tahmo Ground Observational'
        )
        filepath = absolute_path(
            'gap', 'tests', 'ingestor', 'data',
            'TAHMO test stations.zip'
        )
        with open(filepath, 'rb') as _file:
            self.correct_file = SimpleUploadedFile(_file.name, _file.read())

    def _remove_output(self, s3_path, s3, year):
        """Remove parquet output from object storage."""
        s3_storage = storages['gap_products']
        path = (
            f'{s3_path.replace(f's3://{s3['S3_BUCKET_NAME']}/', '')}'
            f'year={year}'
        )
        _, files = s3_storage.listdir(path)
        print(files)
        for file in files:
            s3_storage.delete(file)

    @override_settings(DEBUG=True)
    def test_store(self):
        """Test store method."""
        # TODO: override_settings DEBUG for correct duckdb s3
        session = IngestorSession.objects.create(
            file=self.correct_file,
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False
        )
        session.run()
        session.refresh_from_db()

        data_source = DataSourceFile(name='tahmo_test_store')
        converter = ParquetConverter(
            self.dataset, data_source
        )
        converter.setup()
        converter.run()
        self.assertTrue(converter._check_parquet_exists(
            converter._get_directory_path(data_source),
            2018
        ))

        # use append mode
        converter = ParquetConverter(
            self.dataset, data_source, mode='a'
        )
        converter.setup()
        converter.run()

        self._remove_output(
            converter._get_directory_path(data_source),
            converter._get_s3_variables(),
            2018
        )

    @override_settings(DEBUG=True)
    def test_convert_dataset_to_parquet(self):
        """Test convert_dataset_to_parquet task."""
        # TODO: override_settings DEBUG for correct duckdb s3
        session = IngestorSession.objects.create(
            file=self.correct_file,
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False
        )
        session.run()
        session.refresh_from_db()

        convert_dataset_to_parquet(self.dataset.id)

        data_source = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.PARQUET
        ).last()
        self.assertTrue(data_source)

        converter = ParquetConverter(
            self.dataset, data_source, mode='a'
        )
        converter.setup()
        self.assertTrue(converter._check_parquet_exists(
            converter._get_directory_path(data_source),
            2018
        ))
        self._remove_output(
            converter._get_directory_path(data_source),
            converter._get_s3_variables(),
            2018
        )

    def test_appender_not_run(self):
        """Test run appender not called."""
        data_source = DataSourceFile(name='tahmo_test_store')
        appender = ParquetIngestorAppender(
            self.dataset,
            data_source,
            None,
            datetime.datetime.now()
        )
        appender._process_date_range = mock.MagicMock()
        appender.run()
        appender._process_date_range.assert_not_called()

        appender = WindborneParquetIngestorAppender(
            self.dataset,
            data_source,
            None,
            None
        )
        appender._process_date_range = mock.MagicMock()
        appender.run()
        appender._process_date_range.assert_not_called()

    @override_settings(DEBUG=True)
    def test_appender_run(self):
        """Test run appender successful."""
        session = IngestorSession.objects.create(
            file=self.correct_file,
            ingestor_type=self.ingestor_type,
            trigger_task=False,
            trigger_parquet=False
        )
        session.run()
        session.refresh_from_db()
        data_source = DataSourceFile(name='tahmo_test_store')
        appender = ParquetIngestorAppender(
            self.dataset,
            data_source,
            datetime.datetime.now(),
            datetime.datetime.now() + datetime.timedelta(days=2)
        )
        appender.setup()
        appender._process_date_range = mock.MagicMock()
        appender._process_date_range.return_value = None
        appender.run()
        appender._process_date_range.assert_called_once()
