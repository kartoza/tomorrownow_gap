# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for BackgroundTask Model.
"""

import mock
import datetime
import pytz
from ast import literal_eval as make_tuple
from django.test import TestCase
from django.utils import timezone
from django.conf import settings

from core.models import BackgroundTask, TaskStatus
from core.celery import (
    cancel_task,
    check_ongoing_task,
    update_task_progress,
    is_task_ignored,
    task_sent_handler,
    task_received_handler,
    task_prerun_handler,
    task_success_handler,
    task_failure_handler,
    task_revoked_handler,
    task_internal_error_handler,
    task_retry_handler,
    conf
)
from core.factories import BackgroundTaskF, UserF

from gap.tasks.ingestor import (
    run_ingestor_session, notify_ingestor_failure
)
from gap.tasks.collector import (
    run_collector_session, notify_collector_failure
)
from gap.models.ingestor import IngestorSession, CollectorSession


mocked_dt = datetime.datetime(2024, 8, 14, 10, 10, 10, tzinfo=pytz.UTC)


class RequestObj(object):
    """Mock task request object."""

    def __init__(self, id, name, task_args) -> None:
        """Initialize RequestObj class."""
        self.id = id
        self.name = name
        self.args = task_args


class SenderObj(object):
    """Mock task sender object."""

    def __init__(self, name, request) -> None:
        """Initialize SenderObj class."""
        self.name = name
        self.request = request


@mock.patch(
    'django.utils.timezone.now',
    mock.MagicMock(
        return_value=mocked_dt
    )
)
class TestBackgroundTask(TestCase):
    """Test class for BackgroundTask and its handlers."""

    def setUp(self):
        """Initialize BackgroundTask test class."""
        self.bg_task: BackgroundTask = BackgroundTaskF.create(
            task_name='test-task-1',
            context_id='1'
        )
        self.superuser = UserF.create(
            first_name='admin',
            username='admin@example.com',
            email='admin@example.com',
            is_superuser=True,
            is_active=True
        )

    def get_task_sender(self, bg_task: BackgroundTask):
        """Create sender object from BackgroundTask."""
        request = RequestObj(
            bg_task.task_id,
            bg_task.task_name,
            make_tuple(bg_task.parameters)
        )
        return SenderObj(request.name, request)

    def test_requester_name(self):
        """Test BackgroundTask requester name property."""
        self.assertEqual(self.bg_task.requester_name, 'John Doe')
        user = UserF.create(
            first_name='Test',
            last_name=""
        )
        bg1: BackgroundTask = BackgroundTaskF.create(
            submitted_by=user
        )
        self.assertEqual(bg1.requester_name, 'Test')
        bg2: BackgroundTask = BackgroundTaskF.create(
            submitted_by=None
        )
        self.assertEqual(bg2.requester_name, '-')

    def test_task_on_sent(self):
        """Test task on sent event handler."""
        bg_task = BackgroundTaskF.create(
            task_id='test-id'
        )
        headers = {
            'task': 'test',
            'id': 'test-id'
        }
        task_args = (9999,)
        task_sent_handler(headers=headers, body=(task_args,))
        bg_task.refresh_from_db()
        self.assertTrue(bg_task.task_id)
        headers = {
            'task': self.bg_task.task_name,
            'id': self.bg_task.task_id
        }
        task_args = (9999,)
        task_sent_handler(headers=headers, body=(task_args,))
        self.bg_task.refresh_from_db()
        self.assertEqual(self.bg_task.parameters, str(task_args))
        self.assertEqual(self.bg_task.context_id, '9999')
        self.assertEqual(self.bg_task.status, TaskStatus.PENDING)
        self.assertEqual(
            self.bg_task.last_update, mocked_dt)

    def test_task_on_queued(self):
        """Test task on queued handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_received_handler(sender, sender.request)
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(bg_task.status, TaskStatus.QUEUED)

    def test_task_on_started(self):
        """Test task on started handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_prerun_handler(sender, str(bg_task.task_id))
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(
            bg_task.started_at, mocked_dt)
        self.assertEqual(bg_task.status, TaskStatus.RUNNING)
        self.assertEqual(bg_task.progress, 0)
        self.assertEqual(bg_task.progress_text, 'Task has been started.')

    def test_task_on_completed(self):
        """Test task on completed handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_success_handler(sender, request=sender.request)
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(
            bg_task.finished_at, mocked_dt)
        self.assertEqual(bg_task.status, TaskStatus.COMPLETED)
        self.assertEqual(bg_task.progress, 100)

    def test_task_on_cancelled(self):
        """Test task on cancelleed handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_revoked_handler(sender, request=sender.request)
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(bg_task.status, TaskStatus.CANCELLED)

    def test_task_on_errors(self):
        """Test task on errors handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_failure_handler(
            sender, str(bg_task.task_id),
            exception=Exception('this is error')
        )
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(bg_task.status, TaskStatus.STOPPED)
        self.assertTrue('this is error' in bg_task.errors)

    def test_task_on_internal_error(self):
        """Test task on internal error handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_internal_error_handler(
            sender, str(bg_task.task_id),
            exception=Exception('this is error')
        )
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(bg_task.status, TaskStatus.STOPPED)
        self.assertTrue('this is error' in bg_task.errors)

    def test_task_on_retried(self):
        """Test task on retried handler."""
        bg_task = BackgroundTaskF.create()
        sender = self.get_task_sender(bg_task)
        task_retry_handler(sender, 'test-retry')
        bg_task.refresh_from_db()
        self.assertEqual(
            bg_task.last_update, mocked_dt)
        self.assertEqual(
            bg_task.progress_text, 'Task is retried by scheduler.')

    def test_is_possible_interrupted(self):
        """Test is possible interrupted function."""
        bg_task = BackgroundTaskF.create()
        self.assertFalse(bg_task.is_possible_interrupted())
        bg_task.status = TaskStatus.RUNNING
        bg_task.save()
        self.assertFalse(bg_task.is_possible_interrupted())
        bg_task.last_update = timezone.now() - datetime.timedelta(days=1)
        bg_task.save()
        self.assertTrue(bg_task.is_possible_interrupted())

    def test_conf_celery(self):
        """Test celery conf."""
        conf_data = conf("test")
        self.assertIn('error', conf_data)

    @mock.patch('core.celery.logger.error')
    @mock.patch("core.celery.AsyncResult")
    @mock.patch("core.celery.app.control.revoke")
    def test_cancel_task(self, mocked_revoke, mock_async_result, mock_log):
        """Test cancel task function."""
        mock_result = mock.MagicMock()
        mock_result.ready.return_value = True
        mock_async_result.return_value = mock_result
        cancel_task("empty")
        mocked_revoke.assert_not_called()
        mock_log.assert_not_called()
        # test revoke is called
        mocked_revoke.reset_mock()
        mock_result = mock.MagicMock()
        mock_result.ready.return_value = False
        mock_async_result.return_value = mock_result
        cancel_task("empty")
        mocked_revoke.assert_called_once()
        mock_log.assert_not_called()
        # test throws exception
        mocked_revoke.reset_mock()
        mock_result = mock.MagicMock()
        mock_result.ready.return_value = False
        mock_async_result.return_value = mock_result
        mocked_revoke.side_effect = Exception('Revoke failed')
        cancel_task("empty")
        self.assertEqual(mock_log.call_count, 2)

    def test_check_ongoing_task(self):
        """Test check ongoing task function."""
        self.assertFalse(
            check_ongoing_task("empty", "empty")
        )
        self.assertFalse(
            check_ongoing_task(
                self.bg_task.task_name, self.bg_task.context_id)
        )
        self.bg_task.status = TaskStatus.RUNNING
        self.bg_task.save()
        self.assertTrue(
            check_ongoing_task(
                self.bg_task.task_name, self.bg_task.context_id)
        )

    def test_update_task_progress(self):
        """Test update task progress function."""
        update_task_progress("empty", "empty", 0, None)
        update_task_progress(
            self.bg_task.task_name,
            self.bg_task.context_id,
            77,
            'update-progress'
        )
        self.bg_task.refresh_from_db()
        self.assertEqual(
            self.bg_task.progress, 77
        )
        self.assertEqual(
            self.bg_task.progress_text, 'update-progress'
        )
        self.assertEqual(
            self.bg_task.last_update, mocked_dt
        )

    def test_is_task_ignored(self):
        """Test is task ignored."""
        self.assertTrue(
            is_task_ignored("")
        )
        self.assertTrue(
            is_task_ignored("celery.backend_cleanup")
        )
        self.assertFalse(
            is_task_ignored("test-new-task")
        )

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    def test_task_on_errors_ingestor_session(self, mock_notify):
        """Test that is triggered when ingestor_session fails."""
        bg_task = BackgroundTaskF.create(
            task_name="ingestor_session",
            context_id="10"
        )
        bg_task.task_on_errors(exception="Test ingestor failure")
        mock_notify.assert_called_once_with(10, "Test ingestor failure")

    @mock.patch("gap.tasks.ingestor.notify_ingestor_failure.delay")
    def test_task_on_errors_other_tasks(self, mock_notify):
        """Test do not trigger failure notify."""
        bg_task = BackgroundTaskF.create(
            task_name="random_task",
            context_id="20"
        )
        bg_task.task_on_errors(exception="Test failure")
        mock_notify.assert_not_called()

    @mock.patch("gap.tasks.collector.notify_collector_failure.delay")
    def test_task_on_errors_collector_session_exception(self, mock_notify):
        """Test triggered when CollectorSession fails with an error."""
        bg_task = BackgroundTaskF.create(
            task_name="collector_session",
            context_id="25"
        )

        bg_task.task_on_errors(exception="Test collector failure")

        mock_notify.assert_called_once_with(25, "Test collector failure")

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
