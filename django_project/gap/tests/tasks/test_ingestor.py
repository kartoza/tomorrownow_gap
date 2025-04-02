# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP ingestor tasks.
"""

import mock
from django.test import TestCase
from django.conf import settings

from core.factories import UserF, BackgroundTaskF
from gap.tasks.ingestor import (
    run_daily_ingestor, notify_ingestor_failure,
    run_ingestor_session,
    reset_measurements
)
from gap.models.ingestor import (
    IngestorSession, IngestorSessionStatus
)


class IngestorTaskTest(TestCase):
    """Ingestor task test case."""

    def setUp(self):
        """Initialize IngestorTaskTest test class."""
        self.superuser = UserF.create(
            first_name='admin',
            username='admin@example.com',
            email='admin@example.com',
            is_superuser=True,
            is_active=True
        )

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    def test_task_on_errors_ingestor_session_exception(self, mock_notify):
        """Test triggered when IngestorSession fails with an error."""
        bg_task = BackgroundTaskF.create(
            task_name="ingestor_session",
            context_id="30"
        )

        bg_task.task_on_errors(exception="Test ingestor failure")

        mock_notify.assert_called_once_with(30, "Test ingestor failure")

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    def test_notify_ingestor_failure_session_not_found(self, mock_notify):
        """Test when an ingestor session does not exist."""
        with self.assertLogs("gap.tasks.ingestor", level="WARNING") as cm:
            notify_ingestor_failure(9999, "Session not found")

        self.assertIn("IngestorSession 9999 not found.", cm.output[0])

    @mock.patch("gap.tasks.ingestor.IngestorSession.objects.get")
    @mock.patch("gap.tasks.ingestor.logger")
    def test_notify_ingestor_failure_no_admin_email(
        self, mock_logger, mock_ingestor_get
    ):
        """Test when no admin emails exist."""
        # Mock IngestorSession.objects.get to return a dummy session
        mock_ingestor_get.return_value = mock.Mock()

        self.superuser.is_superuser = False
        self.superuser.save()

        # Call function
        notify_ingestor_failure(42, "Test failure")

        # Verify log message when no admin emails exist
        mock_logger.warning.assert_any_call(
            "No admin email found."
        )

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    @mock.patch("gap.models.ingestor.IngestorSession.objects.get")
    def test_run_ingestor_session_not_found(self, mock_get, mock_notify):
        """Test triggered when session is not found."""
        mock_get.side_effect = IngestorSession.DoesNotExist

        run_ingestor_session(9999)

        mock_notify.assert_called_once_with(9999, "Session not found")

    @mock.patch("gap.tasks.ingestor.send_mail")
    @mock.patch("gap.tasks.ingestor.IngestorSession.objects.get")
    def test_notify_ingestor_failure_with_admin_emails(
        self, mock_ingestor_get, mock_send_mail
    ):
        """Test that email is sent when admin emails exist."""
        # Mock IngestorSession.objects.get to return a dummy session
        ingestor_obj = mock.Mock()
        ingestor_obj.ingestor_type = 'test'
        mock_ingestor_get.return_value = ingestor_obj

        # Call function
        notify_ingestor_failure(42, "Test failure")

        # Ensure send_mail() was called with correct parameters
        mock_send_mail.assert_called_once_with(
            subject="Ingestor Failure Alert",
            message=(
                "Ingestor Session 42 - test has failed.\n\n"
                "Error: Test failure\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=["admin@example.com"],
            fail_silently=False,
        )

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    @mock.patch("gap.models.IngestorSession.objects.create")
    @mock.patch("gap.models.IngestorSession.objects.filter")
    def test_run_daily_ingestor_creates_sessions(
        self, mock_filter, mock_create, mock_notify
    ):
        """Test that new sessions are created when none exist."""
        # No existing session
        mock_filter.return_value.first.return_value = None

        run_daily_ingestor()

        assert mock_create.call_count == 3  # One for each ingestor type
        mock_notify.assert_not_called()  # No failures, so no notifications

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    @mock.patch("gap.models.IngestorSession.objects.create")
    @mock.patch("gap.models.IngestorSession.objects.filter")
    def test_run_daily_ingestor_runs_existing_sessions(
        self, mock_filter, mock_create, mock_notify
    ):
        """Test that existing sessions are run when found."""
        mock_session = mock.MagicMock()
        mock_session.status = IngestorSessionStatus.SUCCESS  # Not failed
        mock_filter.return_value.first.return_value = mock_session

        run_daily_ingestor()

        # All 3 sessions should be run
        assert mock_session.run.call_count == 3
        mock_create.assert_not_called()  # No new sessions should be created
        mock_notify.assert_not_called()  # No failures, so no notifications

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    @mock.patch("gap.models.IngestorSession.objects.create")
    @mock.patch("gap.models.IngestorSession.objects.filter")
    def test_run_daily_ingestor_notifies_on_failure(
        self, mock_filter, mock_create, mock_notify
    ):
        """Test that notify_ingestor_failure is called when session fails."""
        mock_session = mock.MagicMock()
        mock_session.status = IngestorSessionStatus.FAILED
        mock_session.id = 42
        mock_session.notes = "Ingestor failure"
        mock_filter.return_value.first.return_value = mock_session

        run_daily_ingestor()

        assert mock_session.run.call_count == 3  # All sessions should be run
        mock_notify.assert_called_with(42, "Ingestor failure")

    @mock.patch("gap.tasks.ingestor.logger")
    @mock.patch("gap.tasks.ingestor.connection.cursor")
    @mock.patch("gap.models.Dataset.objects.get")
    def test_reset_measurements_valid_dataset(
        self, mock_get_dataset, mock_cursor, mock_logger
    ):
        """Test reset_measurements for a valid dataset."""
        mock_dataset = mock.Mock()
        mock_dataset.name = "Arable Ground Observational"
        mock_dataset.id = 1
        mock_get_dataset.return_value = mock_dataset

        mock_cursor.return_value.__enter__.return_value = mock.Mock()

        reset_measurements(1)

        raw_sql = ("""
        delete from gap_measurement gm
        where id in (
            select gm.id from gap_measurement gm
            join gap_datasetattribute gd on gd.id = gm.dataset_attribute_id
            where gd.dataset_id = %s
        );
        """)

        mock_cursor.return_value.__enter__.\
            return_value.execute.assert_called_once_with(
                raw_sql, [1]
            )
        mock_logger.info.assert_called_once_with(
            "Measurements for dataset Arable Ground Observational "
            "have been reset."
        )

    @mock.patch("gap.tasks.ingestor.logger")
    @mock.patch("gap.models.Dataset.objects.get")
    def test_reset_measurements_invalid_dataset(
        self, mock_get_dataset, mock_logger
    ):
        """Test reset_measurements for an invalid dataset."""
        mock_dataset = mock.Mock()
        mock_dataset.name = "Invalid Dataset"
        mock_get_dataset.return_value = mock_dataset

        with self.assertRaises(ValueError) as context:
            reset_measurements(1)

        self.assertEqual(
            str(context.exception),
            "Invalid dataset Invalid Dataset to be reset!"
        )
        mock_logger.info.assert_not_called()
