# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP collectors tasks.
"""

import mock
from datetime import date
from django.test import TestCase
from django.conf import settings

from core.factories import UserF
from core.models import BackgroundTask
from gap.tasks.collector import (
    run_collector_session, notify_collector_failure,
    check_date_in_collectors, get_existing_collectors,
    run_salient_collector_historical
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

    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    def test_check_date_in_collectors_success(self, mock_collector_get):
        """Test check_date_in_collectors when a matching collector exists."""
        mock_collector = mock.Mock()
        mock_collector.additional_config = {'forecast_date': '2023-01-01'}
        mock_collector.status = IngestorSessionStatus.SUCCESS
        mock_collector.dataset_files.count.return_value = 5
        mock_collector_get.return_value = mock_collector

        result, collector = check_date_in_collectors(
            date(2023, 1, 1), [mock_collector]
        )

        self.assertTrue(result)
        self.assertIsNone(collector)

    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    def test_check_date_in_collectors_failure(self, mock_collector_get):
        """Test check_date_in_collectors when a matching collector fails."""
        mock_collector = mock.Mock()
        mock_collector.additional_config = {'forecast_date': '2023-01-01'}
        mock_collector.status = IngestorSessionStatus.FAILED
        mock_collector.dataset_files.count.return_value = 0
        mock_collector_get.return_value = mock_collector

        result, collector = check_date_in_collectors(
            date(2023, 1, 1), [mock_collector]
        )

        self.assertFalse(result)
        self.assertEqual(collector, mock_collector)

    def test_check_date_in_collectors_no_match(self):
        """Test check_date_in_collectors when no matching collector exists."""
        mock_collector = mock.Mock()
        mock_collector.additional_config = {'forecast_date': '2023-01-02'}
        mock_collector.status = IngestorSessionStatus.SUCCESS
        mock_collector.dataset_files.count.return_value = 5

        result, collector = check_date_in_collectors(
            date(2023, 1, 1), [mock_collector]
        )

        self.assertFalse(result)
        self.assertIsNone(collector)

    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    @mock.patch("gap.tasks.collector.logger")
    def test_get_existing_collectors_success(
        self, mock_logger, mock_collector_get
    ):
        """Test get_existing_collectors when all collectors exist."""
        mock_collector = mock.Mock()
        mock_collector_get.return_value = mock_collector

        collectors = get_existing_collectors([1, 2, 3])

        self.assertEqual(len(collectors), 3)
        mock_collector_get.assert_any_call(id=1)
        mock_collector_get.assert_any_call(id=2)
        mock_collector_get.assert_any_call(id=3)
        mock_logger.error.assert_not_called()

    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    @mock.patch("gap.tasks.collector.logger")
    def test_get_existing_collectors_partial_failure(
        self, mock_logger, mock_collector_get
    ):
        """Test get_existing_collectors when some collectors do not exist."""
        mock_collector = mock.Mock()
        mock_collector_get.side_effect = [
            mock_collector,  # First collector exists
            CollectorSession.DoesNotExist,  # Second collector does not exist
            mock_collector  # Third collector exists
        ]

        collectors = get_existing_collectors([1, 2, 3])

        self.assertEqual(len(collectors), 2)
        mock_collector_get.assert_any_call(id=1)
        mock_collector_get.assert_any_call(id=2)
        mock_collector_get.assert_any_call(id=3)
        mock_logger.error.assert_called_once_with(
            "Existing collector 2 not found."
        )

    @mock.patch("gap.tasks.collector.CollectorSession.objects.get")
    @mock.patch("gap.tasks.collector.logger")
    def test_get_existing_collectors_all_failure(
        self, mock_logger, mock_collector_get
    ):
        """Test get_existing_collectors when no collectors exist."""
        mock_collector_get.side_effect = CollectorSession.DoesNotExist

        collectors = get_existing_collectors([1, 2, 3])

        self.assertEqual(len(collectors), 0)
        mock_collector_get.assert_any_call(id=1)
        mock_collector_get.assert_any_call(id=2)
        mock_collector_get.assert_any_call(id=3)
        mock_logger.error.assert_any_call("Existing collector 1 not found.")
        mock_logger.error.assert_any_call("Existing collector 2 not found.")
        mock_logger.error.assert_any_call("Existing collector 3 not found.")

    @mock.patch("gap.tasks.collector.CollectorSession.dataset_files")
    @mock.patch("gap.tasks.collector.CollectorSession.run")
    @mock.patch("gap.tasks.collector.run_ingestor_session.delay")
    @mock.patch("gap.tasks.collector.get_ingestor_config_from_preferences")
    @mock.patch("gap.tasks.collector.Dataset.objects.get")
    @mock.patch("gap.tasks.collector.logger")
    def test_run_salient_collector_historical(
        self, mock_logger, mock_dataset_get, mock_get_config,
        mock_run_ingestor, mock_collector_run, mock_count
    ):
        """Test run_salient_collector_historical task."""
        mock_dataset = mock.Mock()
        mock_dataset.provider = "test_provider"
        mock_dataset_get.return_value = mock_dataset

        mock_config = {
            "historical_task": {
                "collect_by_year": 2023,
                "remove_temp_file": True,
                "datasourcefile_name": "test_file",
                "datasourcefile_id": 123,
                "datasourcefile_exists": True,
            }
        }
        mock_get_config.return_value = mock_config
        mock_count.count.return_value = 5

        def mocked_run():
            """Run collector that does nothing."""
            pass

        mock_collector_run.side_effect = mocked_run

        run_salient_collector_historical()

        self.assertEqual(mock_collector_run.call_count, 12)
        mock_logger.info.assert_any_call(
            "Collecting historical salient dataset for dates: "
            "[datetime.date(2023, 1, 1), datetime.date(2023, 2, 1), "
            "datetime.date(2023, 3, 1), datetime.date(2023, 4, 1), "
            "datetime.date(2023, 5, 1), datetime.date(2023, 6, 1), "
            "datetime.date(2023, 7, 1), datetime.date(2023, 8, 1), "
            "datetime.date(2023, 9, 1), datetime.date(2023, 10, 1), "
            "datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]"
        )
        mock_run_ingestor.assert_called_once()
