# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Background Task

"""

import logging
import uuid
from ast import literal_eval as make_tuple
from traceback import format_tb
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist

from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger(__name__)


class TaskStatus(models.TextChoices):
    """Task status choices."""

    PENDING = 'Pending', _('Pending')
    QUEUED = 'Queued', _('Queued')
    RUNNING = 'Running', _('Running')
    STOPPED = 'Stopped', _('Stopped with error')
    COMPLETED = 'Completed', _('Completed')
    CANCELLED = 'Cancelled', _('Cancelled')
    INVALIDATED = 'Invalidated', _('Invalidated')


COMPLETED_STATUS = [
    TaskStatus.COMPLETED, TaskStatus.STOPPED, TaskStatus.CANCELLED
]
READ_ONLY_STATUS = [
    TaskStatus.QUEUED, TaskStatus.RUNNING
]


def parse_context_id_from_parameters(parameters: str) -> str:
    """Parse context_id from parameters.

    Context id is assummed to be the first parameter
    :param parameters: string of tuple parameters
    :type parameters: str
    :return: context id
    :rtype: str
    """
    task_param = make_tuple(parameters or '()')
    if len(task_param) == 0:
        return None
    return task_param[0]


class BackgroundTask(models.Model):
    """Class that represents background task."""

    status = models.CharField(
        max_length=255,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING
    )

    task_name = models.CharField(
        max_length=255,
        null=True,
        blank=True
    )

    task_id = models.CharField(
        max_length=256,
        unique=True
    )

    uuid = models.UUIDField(
        default=uuid.uuid4,
        unique=True
    )

    submitted_on = models.DateTimeField(
        default=timezone.now
    )

    submitted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )

    started_at = models.DateTimeField(
        null=True,
        blank=True
    )

    finished_at = models.DateTimeField(
        null=True,
        blank=True
    )

    errors = models.TextField(
        null=True,
        blank=True
    )

    stack_trace_errors = models.TextField(
        null=True,
        blank=True
    )

    progress = models.FloatField(
        null=True,
        blank=True
    )

    progress_text = models.TextField(
        null=True,
        blank=True
    )

    last_update = models.DateTimeField(
        null=True,
        blank=True
    )

    parameters = models.TextField(
        null=True,
        blank=True
    )

    context_id = models.CharField(
        max_length=255,
        null=True,
        blank=True
    )

    running_states = [
        TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING
    ]

    def __str__(self):
        """Get string representation."""
        return str(self.uuid)

    @staticmethod
    def running_tasks():
        """Return all running tasks."""
        return BackgroundTask.objects.filter(
            status__in=BackgroundTask.running_states
        )

    def is_running(self):
        """Check if task is running."""
        return self.status in BackgroundTask.running_states

    @property
    def requester_name(self):
        """Get the requester name."""
        if self.submitted_by and self.submitted_by.first_name:
            name = self.submitted_by.first_name
            if self.submitted_by.last_name:
                name = f'{name} {self.submitted_by.last_name}'
            return name
        return '-'

    def task_on_sent(self, task_id, task_name, parameters):
        """Event handler when task is sent to Celery.

        :param task_id: Celery Task ID
        :type task_id: str
        :param task_name: name of the task
        :type task_name: str
        :param parameters: string of tuple parameters
        :type parameters: str
        """
        self.task_id = task_id
        self.task_name = task_name
        self.parameters = parameters
        self.last_update = timezone.now()
        self.started_at = None
        self.finished_at = None
        self.progress = 0.0
        self.progress_text = None
        self.errors = None
        self.stack_trace_errors = None
        self.context_id = parse_context_id_from_parameters(parameters)
        self.save(
            update_fields=[
                'task_id', 'task_name', 'parameters', 'last_update',
                'started_at', 'finished_at', 'progress', 'progress_text',
                'errors', 'stack_trace_errors', 'context_id'
            ]
        )

    def task_on_queued(self, task_id, task_name, parameters):
        """Event handler when task is placed on worker's queued.

        This event may be skipped when the worker's queue is empty.
        :param task_id: Celery Task ID
        :type task_id: str
        :param task_name: name of the task
        :type task_name: str
        :param parameters: string of tuple parameters
        :type parameters: str
        """
        self.task_id = task_id
        self.task_name = task_name
        self.parameters = parameters
        self.context_id = parse_context_id_from_parameters(parameters)
        self.last_update = timezone.now()
        self.status = TaskStatus.QUEUED
        self.save(
            update_fields=[
                'task_id', 'task_name',
                'parameters', 'last_update', 'status',
                'context_id',
            ]
        )

    def task_on_started(self):
        """Event handler when task is started."""
        self.status = TaskStatus.RUNNING
        self.started_at = timezone.now()
        self.finished_at = None
        self.progress = 0.0
        self.progress_text = 'Task has been started.'
        self.errors = None
        self.stack_trace_errors = None
        self.last_update = timezone.now()
        self.save(update_fields=[
            'status', 'started_at', 'finished_at', 'progress',
            'progress_text', 'last_update', 'errors', 'stack_trace_errors'
        ])

    def task_on_completed(self):
        """Event handler when task is completed."""
        self.last_update = timezone.now()
        self.status = TaskStatus.COMPLETED
        self.finished_at = timezone.now()
        self.progress = 100.0
        self.progress_text = 'Task has been completed.'
        self.save(
            update_fields=['last_update', 'status', 'finished_at',
                           'progress', 'progress_text']
        )

    def task_on_cancelled(self):
        """Event handler when task is cancelled."""
        self.last_update = timezone.now()
        self.status = TaskStatus.CANCELLED
        self.progress_text = 'Task has been cancelled.'
        self.save(
            update_fields=[
                'last_update', 'status', 'progress_text'
            ]
        )

    def task_on_errors(self, exception=None, traceback=None):
        """Event handler when task is stopped with errors.

        :param exception: Exception object, defaults to None
        :type exception: Exception, optional
        :param traceback: Traceback, defaults to None
        :type traceback: Traceback object, optional
        """
        self.last_update = timezone.now()
        self.status = TaskStatus.STOPPED
        ex_msg = str(exception) + '\n'
        if traceback:
            try:
                ex_msg += "\n".join(format_tb(traceback))
            except Exception:
                if isinstance(traceback, str):
                    ex_msg += str(traceback) + '\n'
        self.errors = str(exception) + '\n'
        self.stack_trace_errors = ex_msg
        self.progress_text = 'Task is stopped with errors.'
        logger.error(f'Task {self.task_name} is stopped with errors.')
        logger.error(str(exception))
        self.save(
            update_fields=[
                'last_update', 'status', 'errors', 'stack_trace_errors',
                'progress_text'
            ]
        )
        if self.task_name in {"ingestor_session", "collector_session"}:
            session_id = int(self.context_id)

            if self.task_name == "ingestor_session":
                from gap.tasks.ingestor import notify_ingestor_failure
                notify_ingestor_failure.delay(session_id, str(exception))

            else:  # "collector_session"
                from gap.tasks.collector import notify_collector_failure
                notify_collector_failure.delay(session_id, str(exception))

    def task_on_retried(self, reason):
        """Event handler when task is retried by celery.

        :param reason: description why it's being retried
        :type reason: str
        """
        self.last_update = timezone.now()
        self.progress_text = 'Task is retried by scheduler.'
        self.save(
            update_fields=['last_update', 'progress_text']
        )

    def is_possible_interrupted(self, delta=1800):
        """Check whether the task is stuck or being interrupted.

        This requires the task to send an update to BackgroundTask.
        :param delta: Diff seconds, defaults to 1800
        :type delta: int, optional
        :return: True if task is stuck or interrupted
        :rtype: bool
        """
        if (
                self.status == TaskStatus.QUEUED or
                self.status == TaskStatus.RUNNING
        ):
            # check if last_update is more than 30mins than current date time
            if self.last_update:
                diff_seconds = timezone.now() - self.last_update
                return diff_seconds.total_seconds() >= delta
        return False

    def is_task_completed(self, check_context=True):
        """Check if task is completed."""
        result = self.status == TaskStatus.COMPLETED
        if not result:
            return result

        if check_context and self.context_id:
            context_model = TaskContextIdModel.objects.filter(
                task_name=self.task_name
            ).first()
            if not context_model:
                return result

            obj = context_model.get_object(self.context_id)
            # TODO: check if obj status is also completed without error

        return result


class TaskContextIdModel(models.Model):
    """Class that represents the mapping between Task and Context Model."""

    task_name = models.CharField(max_length=255)
    model_name = models.CharField(max_length=512)

    def get_object(self, context_id):
        """Get object by model name and context id."""
        try:
            # Get the model class from the string
            model_string = self.model_name
            app_label, model_name = model_string.split(".")
            model_class = apps.get_model(app_label, model_name)

            if not model_class:
                raise LookupError(f"Model '{model_string}' not found.")

            # Retrieve the object
            obj = model_class.objects.get(pk=context_id)
            return obj
        except ObjectDoesNotExist:
            return None  # or handle not found case
        except Exception as e:
            raise e
