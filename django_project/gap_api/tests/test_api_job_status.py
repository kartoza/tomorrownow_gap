# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Job Status API.
"""

import uuid
from unittest.mock import patch
from rest_framework import status
from django.urls import reverse

from core.tests.common import BaseAPIViewTest, FakeResolverMatchV1
from core.models.background_task import TaskStatus
from django_project.gap_api.factories import UserFileFactory
from gap_api.models import Job
from gap_api.api_views.measurement import JobStatusAPI


class TestJobStatusAPI(BaseAPIViewTest):
    """Test case for Job Status API."""

    def setUp(self):
        """Init test class."""
        super().setUp()
        self.job = Job.objects.create(
            status=TaskStatus.PENDING,
            user=self.user_1,
        )
        self.url, self.default_kwargs = self._get_url(self.job.uuid)
        self.view = JobStatusAPI.as_view()

    def _get_url(self, job_id):
        kwargs = {'job_id': str(job_id)}
        return reverse(
            'api:v1:measurement-job-status',
            kwargs=kwargs
        ), kwargs

    def test_get_job_status_success(self):
        """Test retrieving job status successfully."""
        request = self.factory.get(self.url)
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        response = self.view(request, **self.default_kwargs)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('status', response.data)
        self.assertEqual(response.data['status'], self.job.status)
        self.assertIsNone(response.data.get('errors'))
        self.assertIsNone(response.data.get('data'))
        self.assertIsNone(response.data.get('url'))

    def test_get_job_status_not_found(self):
        """Test retrieving job status for a non-existent job."""
        url, kwargs = self._get_url(str(uuid.uuid4()))  # Non-existent job ID
        request = self.factory.get(url)
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        response = self.view(request, **kwargs)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch('gap_api.api_views.measurement.Job.objects.get')
    def test_get_job_status_exception(self, mock_get):
        mock_get.side_effect = Exception("Unexpected error")
        request = self.factory.get(self.url)
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        # Simulate an internal server error
        with self.assertRaises(Exception):
            self.view(request, **self.default_kwargs)

    def test_job_with_json_data(self):
        """Test retrieving job status with JSON data."""
        self.job.status = TaskStatus.COMPLETED
        self.job.output_json = {'result': 'success'}
        self.job.save()

        request = self.factory.get(self.url)
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        response = self.view(request, **self.default_kwargs)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('status', response.data)
        self.assertEqual(response.data['status'], TaskStatus.COMPLETED)
        self.assertIn('data', response.data)
        self.assertEqual(response.data['data'], {'result': 'success'})

    def test_job_with_file_url(self):
        """Test retrieving job status with file URL."""
        user_file = UserFileFactory.create(
            user=self.user_1,
            name='dev/user_data/file.csv',
        )
        user_file.save()
        self.job.status = TaskStatus.COMPLETED
        self.job.output_file = user_file
        self.job.save()

        request = self.factory.get(self.url)
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        response = self.view(request, **self.default_kwargs)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('status', response.data)
        self.assertEqual(response.data['status'], TaskStatus.COMPLETED)
        self.assertIn('url', response.data)
        self.assertIn('file.csv', response.data['url'])
