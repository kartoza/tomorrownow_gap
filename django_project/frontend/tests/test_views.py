# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for Views.
"""

from django.test import Client
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase, override_settings
from rest_framework.test import APIRequestFactory, force_authenticate
from unittest.mock import MagicMock, patch

from dcas.models.request import DCASRequest
from dcas.models.output import DCASOutput, DCASDeliveryMethod
from dcas.models.download_log import DCASDownloadLog
from frontend.models import PagePermission
from frontend.views import OutputDownloadView
from core.tests.common import BaseAPIViewTest


class TestHomeView(BaseAPIViewTest):
    """Test HomeView class."""

    def test_home_view(self):
        """Test Home View."""
        c = Client()
        response = c.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.has_header('Content-Type'))
        self.assertIn('text/html', response.headers['Content-Type'])
        self.assertIn('gap_base_context', response.context)


User = get_user_model()


class OutputDownloadViewTest(TestCase):
    """Tests for DCAS OutputDownloadView."""

    @classmethod
    def setUpTestData(cls):
        """Set up test data for OutputDownloadView tests."""
        cls.factory = APIRequestFactory()
        # create a DCAS request and output
        cls.dcas_request = DCASRequest.objects.create()
        cls.output = DCASOutput.objects.create(
            request=cls.dcas_request,
            path="some/path/to.csv",
            file_name="the.csv",
            delivery_by=DCASDeliveryMethod.OBJECT_STORAGE,
            size=123,
        )
        # users
        cls.regular = User.objects.create_user(
            username="u", email="u@example.com", password="pass"
        )
        cls.kalro_group, _ = Group.objects.get_or_create(name="Kalro")
        cls.kalro = User.objects.create_user(
            username="k", email="k@example.com", password="pass"
        )
        cls.kalro.groups.add(cls.kalro_group)
        cls.superuser = User.objects.create_superuser(
            username="s", email="s@example.com", password="pass"
        )
        # create a permission for the kalro group
        perm = PagePermission.objects.create(page="dcas_csv")
        perm.groups.add(cls.kalro_group)

    def test_permission_denied_for_anonymous(self):
        """Test that anonymous users are denied access to download."""
        request = self.factory.get(f"/outputs/{self.output.pk}/download/")
        response = OutputDownloadView.as_view()(request, pk=self.output.pk)
        self.assertEqual(response.status_code, 401)

    def test_permission_denied_for_regular_user(self):
        """Test that regular users are denied access to download."""
        request = self.factory.get(f"/outputs/{self.output.pk}/download/")
        force_authenticate(request, user=self.regular)
        response = OutputDownloadView.as_view()(request, pk=self.output.pk)
        self.assertEqual(response.status_code, 403)

    @override_settings(DEBUG=False)
    @patch("frontend.views.storages")
    def test_returns_signed_url_and_logs_download(self, storages_mock):
        """Test that KALRO users can download output and log is created."""
        fake_storage = MagicMock()
        fake_storage.url.return_value = "https://signed-url/"
        storages_mock.__getitem__.return_value = fake_storage

        request = self.factory.get(f"/outputs/{self.output.pk}/download/")
        force_authenticate(request, user=self.kalro)
        response = OutputDownloadView.as_view()(request, pk=self.output.pk)

        # assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {"url": "https://signed-url/"})
        fake_storage.url.assert_called_once_with(
            name=self.output.path,
            expire=900,
            parameters={
                "ResponseContentDisposition":
                    f'attachment; filename="{self.output.file_name}"'
            },
        )
        # audit row created
        log = DCASDownloadLog.objects.get(output=self.output, user=self.kalro)
        self.assertIsNotNone(log)

    @override_settings(DEBUG=True)
    @patch("frontend.views.storages")
    def test_debug_rewrites_host(self, storages_mock):
        """Test that in DEBUG mode, the URL host is rewritten."""
        fake_storage = MagicMock()
        fake_storage.url.return_value = "http://minio:9000/bucket/file.csv"
        storages_mock.__getitem__.return_value = fake_storage

        request = self.factory.get(f"/outputs/{self.output.pk}/download/")
        force_authenticate(request, user=self.superuser)

        response = OutputDownloadView.as_view()(request, pk=self.output.pk)

        # DEBUG replaces minio host with localhost:9010
        self.assertEqual(
            response.data["url"],
            "http://localhost:9010/bucket/file.csv"
        )
