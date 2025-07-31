# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for SPW Tamsat.
"""

from datetime import date
from django.test import TestCase

from core.models.background_task import TaskStatus
from spw.models import SPWExecutionLog, SPWMethod
from gap.factories import FarmGroupFactory


class SPWExecutionLogTest(TestCase):
    """Test case for SPWExecutionLog model."""

    fixtures = [
        '1.object_storage_manager.json',
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def test_create_log(self):
        """Test creating a log entry."""
        farm_group = FarmGroupFactory()
        log = SPWExecutionLog.init_record(
            farm_group,
            date.fromisoformat('2023-10-01'),
            SPWMethod.TAMSAT
        )
        self.assertIsInstance(log, SPWExecutionLog)
        self.assertEqual(log.status, TaskStatus.PENDING)
        # test update
        log2 = SPWExecutionLog.init_record(
            farm_group,
            date.fromisoformat('2023-10-01'),
            SPWMethod.TAMSAT
        )
        self.assertEqual(log2.id, log.id)
        log2.start()
        self.assertEqual(log2.status, TaskStatus.RUNNING)
        log2.stop_with_error("Test error message")
        self.assertEqual(log2.status, TaskStatus.STOPPED)
        self.assertEqual(log2.errors, "Test error message")
        log2.success()
        self.assertEqual(log2.status, TaskStatus.COMPLETED)
