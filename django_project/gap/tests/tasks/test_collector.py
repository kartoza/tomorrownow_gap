# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP collectors tasks.
"""

import mock
from django.test import TestCase
from django.conf import settings

from core.factories import UserF
from core.models import BackgroundTask
from gap.tasks.collector import (
    run_collector_session, notify_collector_failure
)
from gap.models.ingestor import (
    CollectorSession, IngestorSessionStatus
)


class CollectorTaskTest(TestCase):
    """Collector task test case."""

    def setUp(self):
        """Initialize CollectorTaskTest test class."""
        self.superuser = UserF.create(
            first_name='admin',
            username='admin@example.com',
            email='admin@example.com',
            is_superuser=True,
            is_active=True
        )

    @mock.patch("gap.models.ingestor.CollectorSession.objects.get")
    @mock.patch("gap.tasks.collector.notify_collector_failure.delay")
    def test_run_collector_session_not_found(self, mock_notify, mock_get):
        """Test triggered when collector session is not found."""
        mock_get.side_effect = CollectorSession.DoesNotExist

        run_collector_session(9999)

        mock_notify.assert_called_once_with(
            9999, "Collector session not found"
        )

    @mock.patch("gap.tasks.collector.notify_collector_failure.delay")
    def test_task_on_errors_collector_session(self, mock_notify):
        """Test triggered when collector_session fails."""
        bg_task = BackgroundTask.objects.create(
            task_name="collector_session",
            context_id="15"
        )
        bg_task.task_on_errors(exception="Test collector failure")

        mock_notify.assert_called_once_with(15, "Test collector failure")

    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    @mock.patch("gap.tasks.collector.logger")
    def test_notify_collector_failure_no_admin_email(
        self, mock_logger, mock_collector_get
    ):
        """Test when no admin emails exist for collector failure."""
        mock_collector_get.return_value = mock.Mock()

        self.superuser.is_superuser = False
        self.superuser.save()

        notify_collector_failure(42, "Test failure")

        mock_logger.warning.assert_any_call(
            "No admin email found."
        )

    @mock.patch("gap.tasks.collector.send_mail")
    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    def test_notify_collector_failure_with_admin_emails(
        self, mock_collector_get, mock_send_mail
    ):
        """Test that email is sent for collector failure."""
        ingestor_obj = mock.Mock()
        ingestor_obj.ingestor_type = 'test'
        mock_collector_get.return_value = ingestor_obj

        notify_collector_failure(42, "Test failure")

        mock_send_mail.assert_called_once_with(
            subject="Collector Failure Alert",
            message=(
                "Collector Session 42 - test has failed.\n\n"
                "Error: Test failure\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=["admin@example.com"],
            fail_silently=False,
        )

    @mock.patch("gap.tasks.collector.notify_collector_failure.delay")
    @mock.patch("gap.models.CollectorSession.objects.get")
    def test_run_collector_session_failed_status(self, mock_get, mock_notify):
        """Test when session has FAILED status."""
        mock_session = mock.MagicMock()
        mock_session.run.return_value = None  # No exception
        mock_session.status = IngestorSessionStatus.FAILED
        mock_session.notes = "Failure reason"
        mock_get.return_value = mock_session

        run_collector_session(1)

        mock_notify.assert_called_once_with(1, "Failure reason")
