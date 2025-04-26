# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for tasks.
"""

from django.test import TestCase

from unittest.mock import patch, MagicMock, ANY
from django.utils import timezone

from core.models.background_task import TaskStatus
from core.tasks import check_running_tasks


class TestCheckRunningTasks(TestCase):
    """Unit tests for check_running_tasks."""

    @patch('core.tasks.send_mail')
    @patch('core.tasks.get_admin_emails')
    @patch('core.tasks.BackgroundTask.objects.filter')
    def test_check_running_tasks_with_running_tasks(
        self, mock_filter, mock_get_admin_emails, mock_send_mail
    ):
        """Test check_running_tasks with running tasks."""
        # Mock data
        mock_task = MagicMock()
        mock_task.task_name = 'generate_insight_report'
        mock_task.task_id = '12345'
        mock_task.started_at = timezone.now() - timezone.timedelta(hours=7)
        mock_task.parameters = '{"param1": "value1"}'
        mock_filter.return_value = [mock_task]

        mock_get_admin_emails.return_value = ['admin@example.com']

        # Call the task
        check_running_tasks()

        # Assertions
        mock_filter.assert_called_once_with(
            task_name__in=[
                'generate_insight_report',
                'ingestor_session',
                'collector_session',
                'run_dcas',
                'run_daily_ingestor',
                'retry_crop_plan_generators',
                'tio_collector_session',
                'salient_collector_session',
            ],
            status=TaskStatus.RUNNING,
            started_at__lt=ANY,
            started_at__date=timezone.now().date(),
        )
        mock_send_mail.assert_called_once_with(
            subject="Running Tasks Alert",
            message=ANY,
            from_email=ANY,
            recipient_list=['admin@example.com'],
            fail_silently=False,
        )

    @patch('core.tasks.send_mail')
    @patch('core.tasks.get_admin_emails')
    @patch('core.tasks.BackgroundTask.objects.filter')
    def test_check_running_tasks_without_running_tasks(
        self, mock_filter, mock_get_admin_emails, mock_send_mail
    ):
        """Test check_running_tasks without running tasks."""
        # Mock no running tasks
        mock_filter.return_value = []
        mock_get_admin_emails.return_value = ['admin@example.com']

        # Call the task
        check_running_tasks()

        # Assertions
        mock_filter.assert_called_once()
        mock_send_mail.assert_not_called()

    @patch('core.tasks.send_mail')
    @patch('core.tasks.get_admin_emails')
    @patch('core.tasks.BackgroundTask.objects.filter')
    def test_check_running_tasks_without_admin_emails(
        self, mock_filter, mock_get_admin_emails, mock_send_mail
    ):
        """Test check_running_tasks without admin emails."""
        # Mock data
        mock_task = MagicMock()
        mock_task.task_name = 'generate_insight_report'
        mock_task.task_id = '12345'
        mock_task.started_at = timezone.now() - timezone.timedelta(hours=7)
        mock_task.parameters = '{"param1": "value1"}'
        mock_filter.return_value = [mock_task]

        # No admin emails
        mock_get_admin_emails.return_value = []

        # Call the task
        check_running_tasks()

        # Assertions
        mock_filter.assert_called_once()
        mock_send_mail.assert_not_called()
