# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline tasks.
"""

import os
import csv
import tempfile
from mock import patch, MagicMock
import datetime
import pytz
import pandas as pd

from gap.models import TaskStatus, Preferences
from dcas.models import (
    DCASRequest,
    DCASOutput,
    DCASDeliveryMethod,
    DCASErrorLog,
    DCASErrorType
)
from dcas.tests.base import DCASPipelineBaseTest
from dcas.tasks import (
    DCASPreferences,
    export_dcas_minio,
    export_dcas_sftp,
    run_dcas,
    log_farms_without_messages
)
from gap.factories import FarmRegistryGroupFactory


class DCASPipelineTaskTest(DCASPipelineBaseTest):
    """DCAS Pipeline tasks test case."""

    def test_dcas_preferences(self):
        """Test DCASPreferences."""
        # monday
        current_dt = datetime.date(2025, 1, 27)

        # with default config from preferences
        dcas_config = DCASPreferences(current_dt)
        self.assertEqual(dcas_config.request_date, current_dt)
        self.assertFalse(dcas_config.is_scheduled_to_run)
        self.assertEqual(len(dcas_config.farm_registry_groups), 0)
        self.assertFalse(dcas_config.farm_num_partitions)
        self.assertFalse(dcas_config.grid_crop_num_partitions)
        self.assertEqual(dcas_config.duck_db_num_threads, 2)
        self.assertFalse(dcas_config.store_csv_to_minio)
        self.assertFalse(dcas_config.store_csv_to_sftp)
        self.assertEqual(
            dcas_config.object_storage_path('test.csv'),
            'dev/dcas_csv/test.csv'
        )

    @patch('django.core.files.storage.base.Storage.save')
    @patch('dcas.outputs.DCASPipelineOutput.convert_to_csv')
    def test_export_dcas_output_minio(
        self, mocked_convert_csv, mocked_storage
    ):
        """Test export_dcas_output minio."""
        # create request
        request = DCASRequest.objects.create(
            requested_at=datetime.datetime(
                2025, 1, 27, 0, 0, 0,
                tzinfo=pytz.UTC
            )
        )

        filename = None
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp_file:
            with open(tmp_file.name, 'w', newline='') as file:
                writer = csv.writer(file)
                field = ["farmerId", "crop", "plantingDate"]
                writer.writerow(field)
            mocked_convert_csv.return_value = tmp_file.name
            mocked_storage.return_value = True
            filename = os.path.basename(tmp_file.name)

            export_dcas_minio(request.id)

        mocked_convert_csv.assert_called_once()
        mocked_storage.assert_called_once()
        # assert the dcas output has been created
        check_output = DCASOutput.objects.filter(
            request=request,
            delivery_by=DCASDeliveryMethod.OBJECT_STORAGE
        ).first()
        self.assertTrue(check_output)
        self.assertEqual(check_output.file_name, filename)
        self.assertIn(filename, check_output.path)

    @patch('dcas.outputs.DCASPipelineOutput.upload_to_sftp')
    @patch('dcas.outputs.DCASPipelineOutput.convert_to_csv')
    def test_export_dcas_output_sftp(self, mocked_convert_csv, mocked_sftp):
        """Test export_dcas_output sftp."""
        # create request
        request = DCASRequest.objects.create(
            requested_at=datetime.datetime(
                2025, 1, 27, 0, 0, 0,
                tzinfo=pytz.UTC
            )
        )

        filename = None
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp_file:
            with open(tmp_file.name, 'w', newline='') as file:
                writer = csv.writer(file)
                field = ["farmerId", "crop", "plantingDate"]
                writer.writerow(field)
            mocked_convert_csv.return_value = tmp_file.name
            mocked_sftp.return_value = True
            filename = os.path.basename(tmp_file.name)

            export_dcas_sftp(request.id)

        mocked_convert_csv.assert_called_once()
        mocked_sftp.assert_called_once()
        # assert the dcas output has been created
        check_output = DCASOutput.objects.filter(
            request=request,
            delivery_by=DCASDeliveryMethod.SFTP
        ).first()
        self.assertTrue(check_output)
        self.assertEqual(check_output.file_name, filename)
        self.assertIn(filename, check_output.path)

    @patch('django.utils.timezone.now')
    @patch('dcas.pipeline.DCASDataPipeline.run')
    def test_run_dcas_skip_weekday(self, mocked_run, mocked_timezone):
        """Test run_dcas."""
        # Monday
        dt = datetime.datetime(
            2025, 1, 27, 0, 0, 0,
            tzinfo=pytz.UTC
        )
        mocked_timezone.return_value = dt
        mocked_run.return_value = True
        run_dcas()
        mocked_run.assert_not_called()
        check_request = DCASRequest.objects.filter(
            requested_at=dt,
            status=TaskStatus.PENDING
        ).first()
        self.assertTrue(check_request)
        self.assertIn('skipping weekday', check_request.progress_text)

    @patch('django.utils.timezone.now')
    @patch('dcas.pipeline.DCASDataPipeline.run')
    def test_run_dcas_empty_farm_registry(self, mocked_run, mocked_timezone):
        """Test run_dcas."""
        # Monday
        dt = datetime.datetime(
            2025, 1, 27, 0, 0, 0,
            tzinfo=pytz.UTC
        )
        mocked_timezone.return_value = dt
        mocked_run.return_value = True

        # update preferences without any farm_registries
        farm_group = FarmRegistryGroupFactory()
        preferences = Preferences.load()
        preferences.dcas_config = {
            'weekdays': [dt.date().weekday()],
            'override_request_date': dt.date().isoformat(),
            'farm_registries': []
        }
        preferences.save()

        run_dcas()
        mocked_run.assert_not_called()
        check_request = DCASRequest.objects.filter(
            requested_at=dt,
            status=TaskStatus.PENDING
        ).first()
        self.assertTrue(check_request)
        self.assertIn('No farm registry group', check_request.progress_text)
        check_request.delete()

        # with farm_registry id
        preferences.dcas_config = {
            'weekdays': [dt.date().weekday()],
            'override_request_date': dt.date().isoformat(),
            'farm_registries': [farm_group.id]
        }
        preferences.save()

        run_dcas()
        mocked_run.assert_not_called()
        check_request = DCASRequest.objects.filter(
            requested_at=dt,
            status=TaskStatus.PENDING
        ).first()
        self.assertTrue(check_request)
        self.assertIn(
            'No farm registry in the registry groups',
            check_request.progress_text
        )

    @patch('django.utils.timezone.now')
    @patch('dcas.pipeline.DCASDataPipeline.run')
    def test_run_dcas_success(self, mocked_run, mocked_timezone):
        """Test run_dcas."""
        # Tuesday
        dt = datetime.datetime(
            2025, 1, 28, 0, 0, 0,
            tzinfo=pytz.UTC
        )
        mocked_timezone.return_value = dt
        mocked_run.return_value = True

        preferences = Preferences.load()
        preferences.dcas_config = {
            'weekdays': [dt.date().weekday()],
            'override_request_date': dt.date().isoformat(),
            'farm_registries': [self.farm_registry_group.id]
        }
        preferences.save()

        run_dcas()
        mocked_run.assert_called_once()
        check_request = DCASRequest.objects.filter(
            requested_at=dt,
            status=TaskStatus.COMPLETED
        ).first()
        self.assertTrue(check_request)

    @patch("duckdb.connect")
    def test_log_farms_without_messages(self, mocked_duck_db):
        """Test log_farms_without_messages."""
        # create request
        request = DCASRequest.objects.create(
            requested_at=datetime.datetime(
                2025, 1, 27, 0, 0, 0,
                tzinfo=pytz.UTC
            )
        )

        # Mock DuckDB return DataFrames (Simulating chunked retrieval)
        chunk_1 = pd.DataFrame(
            {
                'farm_id': [
                    self.farm_registry_1.farm.id,
                    self.farm_registry_2.farm.id
                ],
                'crop': ['Maize Early', 'Cassava Mid'],
                'farm_unique_id': [1, 2],
                'growth_stage': ['testA', 'testB']
            }
        )

        expected_chunks = [chunk_1]

        # Configure mock connection to return chunks in order
        conn = MagicMock()
        conn.sql.return_value.df.side_effect = expected_chunks
        mocked_duck_db.return_value = conn

        # run error handling
        log_farms_without_messages(request.id, 2)

        error_logs = DCASErrorLog.objects.filter(
            request=request,
            error_type=DCASErrorType.MISSING_MESSAGES
        )
        self.assertEqual(error_logs.count(), 2)
        error_log1 = error_logs.filter(farm=self.farm_registry_1.farm).first()
        self.assertTrue(error_log1)
        self.assertIn('Farm 1', error_log1.error_message)
        error_log2 = error_logs.filter(farm=self.farm_registry_2.farm).first()
        self.assertTrue(error_log2)
        self.assertIn('Farm 2', error_log2.error_message)
