# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Location API.
"""

from django.test import TestCase
from unittest.mock import patch, MagicMock, PropertyMock

from core.models import TaskStatus
from gap_api.models.job import Job
from gap_api.tasks.job import BaseJobExecutor


class TestBaseJobExecutor(TestCase):
    """Unit tests for BaseJobExecutor class."""

    def setUp(self):
        """Initialize test class."""
        self.mock_job = MagicMock(spec=Job)
        self.instance = BaseJobExecutor(self.mock_job)
        self.instance.job = self.mock_job
        self.instance.wait_time = 10.0
        self.instance.wait_sleep = 1.0

    def test_run_raises_not_implemented(self):
        """Test that run method raises NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.instance.run()

    @patch('gap_api.tasks.job.logger')
    @patch('time.sleep')
    @patch('time.time')
    def test_job_completes_successfully(
        self, mock_time, mock_sleep, mock_logger
    ):
        """Test that method returns True when job completes successfully."""
        # Mock time progression: start at 0, then 2 seconds later,
        # then 4 seconds
        mock_time.side_effect = [0.0, 2.0, 4.0]
        self.mock_job.wait_type = 1

        # Start with RUNNING status
        status_sequence = [
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED
        ]
        type(self.mock_job).status = PropertyMock(side_effect=status_sequence)

        result = self.instance._wait_for_completion()

        self.assertTrue(result)
        self.assertEqual(self.mock_job.refresh_from_db.call_count, 2)
        mock_sleep.assert_called_once_with(1.0)
        mock_logger.warning.assert_not_called()

    @patch('gap_api.tasks.job.logger')
    @patch('time.sleep')
    @patch('time.time')
    def test_job_times_out(self, mock_time, mock_sleep, mock_logger):
        """Test that method returns False and logs warning when times out."""
        # Mock time progression that exceeds wait_time
        mock_time.side_effect = [0.0, 5.0, 11.0]  # Exceeds 10 second wait_time
        self.mock_job.wait_type = 1
        self.mock_job.uuid = 'test-uuid-123'

        # Job status stays RUNNING throughout (never completes)
        status_sequence = [TaskStatus.RUNNING, TaskStatus.RUNNING]
        type(self.mock_job).status = PropertyMock(side_effect=status_sequence)

        result = self.instance._wait_for_completion()

        self.assertFalse(result)
        self.assertEqual(self.mock_job.refresh_from_db.call_count, 1)
        mock_sleep.assert_called_with(1.0)
        mock_logger.warning.assert_called_once_with(
            "Job test-uuid-123 did not complete within the wait time 10.0."
        )
