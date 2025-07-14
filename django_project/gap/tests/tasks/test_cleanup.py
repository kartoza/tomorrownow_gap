# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP cleanup tasks.
"""

import datetime
from unittest.mock import patch
from django.test import TestCase

from gap.models import (
    SignUpRequest,
    RequestStatus,
    DataSourceFile,
    DataSourceFileRententionConfig
)
from gap.tasks.cleanup import (
    cleanup_incomplete_signups,
    cleanup_deleted_zarr
)
from gap.factories import DataSourceFileFactory


class IncompleteSignupsCleanupTest(TestCase):
    """Test case for cleanup of incomplete signups."""

    @patch("gap.tasks.cleanup.timezone.now")
    def test_successful_cleanup(self, mock_now):
        """Test cleanup of old incomplete signups."""
        mock_now.return_value = datetime.datetime(2023, 10, 1)

        # Create a mock SignUpRequest with INCOMPLETE status
        request1 = SignUpRequest.objects.create(
            first_name="Test",
            last_name="User1",
            email="test.user1@example.com",
            description="Test description",
            status=RequestStatus.INCOMPLETE,
        )
        request1.submitted_at = datetime.datetime(2023, 8, 30)
        request1.save()
        request2 = SignUpRequest.objects.create(
            first_name="Test",
            last_name="User2",
            email="test.user2@example.com",
            description="Test description",
            status=RequestStatus.PENDING,
        )
        request2.submitted_at = datetime.datetime(2023, 8, 30)
        request2.save()

        # Call the cleanup task
        cleanup_incomplete_signups()

        # Verify that the incomplete request was deleted
        self.assertFalse(
            SignUpRequest.objects.filter(id=request1.id).exists()
        )
        # Verify that the pending request was not deleted
        self.assertTrue(SignUpRequest.objects.filter(id=request2.id).exists())


class DeletedZarrCleanupTest(TestCase):
    """Test case for cleanup of deleted Zarr files."""

    fixtures = [
        '1.object_storage_manager.json',
    ]

    @patch("gap.models.dataset.timezone.now")
    def test_datasource_should_delete(self, mock_now):
        """Test if DataSourceFile should be deleted."""
        ds_file_no_delete = DataSourceFileFactory.create(
            name="test.zarr",
            format="ZARR",
            deleted_at=None,
            is_latest=False,
            metadata={}
        )
        self.assertFalse(ds_file_no_delete.should_delete())

        mock_now.return_value = datetime.datetime(2023, 10, 1)
        ds_file = DataSourceFileFactory.create(
            name="test.zarr",
            format="ZARR",
            deleted_at=datetime.datetime(2023, 9, 15),
            is_latest=False,
            metadata={}
        )
        # add DataSourceFileRententionConfig
        DataSourceFileRententionConfig.objects.create(
            dataset=ds_file.dataset,
            days_to_keep=7
        )
        self.assertTrue(ds_file.should_delete())

    @patch("core.utils.s3.remove_s3_folder_by_batch")
    @patch("gap.tasks.cleanup.timezone.now")
    def test_successful_cleanup_empty(self, mock_now, mock_remove_s3_folder):
        """Test cleanup of old deleted Zarr files."""
        mock_now.return_value = datetime.datetime(2023, 10, 1)
        ds_file = DataSourceFileFactory.create(
            name="test.zarr",
            format="ZARR",
            deleted_at=None,
            is_latest=False,
            metadata={}
        )

        count, failed_count = cleanup_deleted_zarr()
        self.assertEqual(count, 0)
        self.assertEqual(failed_count, 0)
        mock_remove_s3_folder.assert_not_called()
        self.assertTrue(
            DataSourceFile.objects.filter(id=ds_file.id).exists()
        )

    @patch("core.utils.s3.remove_s3_folder_by_batch")
    @patch("gap.tasks.cleanup.timezone.now")
    def test_successful_cleanup_being_skipped(
        self, mock_now, mock_remove_s3_folder
    ):
        """Test cleanup of old deleted Zarr files."""
        mock_now.return_value = datetime.datetime(
            2023, 10, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        ds_file = DataSourceFileFactory.create(
            name="test.zarr",
            format="ZARR",
            deleted_at=datetime.datetime(
                2023, 10, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            is_latest=False,
            metadata={}
        )

        count, failed_count = cleanup_deleted_zarr()
        self.assertEqual(count, 0)
        self.assertEqual(failed_count, 0)
        mock_remove_s3_folder.assert_not_called()
        self.assertTrue(
            DataSourceFile.objects.filter(id=ds_file.id).exists()
        )

    @patch("core.utils.s3.remove_s3_folder_by_batch")
    @patch("gap.tasks.cleanup.timezone.now")
    def test_successful_cleanup_success(
        self, mock_now, mock_remove_s3_folder
    ):
        """Test cleanup of old deleted Zarr files."""
        mock_now.return_value = datetime.datetime(
            2023, 10, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        ds_file = DataSourceFileFactory.create(
            name="test.zarr",
            format="ZARR",
            deleted_at=datetime.datetime(
                2023, 9, 18, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            is_latest=False,
            metadata={}
        )
        ds_file2 = DataSourceFileFactory.create(
            name="test2.zarr",
            format="ZARR",
            deleted_at=datetime.datetime(
                2023, 10, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            is_latest=False,
            metadata={}
        )

        mock_remove_s3_folder.return_value = {
            'total_deleted': 1,
            'total_batches': 1
        }
        count, failed_count = cleanup_deleted_zarr()
        self.assertEqual(count, 1)
        self.assertEqual(failed_count, 0)
        mock_remove_s3_folder.assert_called_once()
        self.assertTrue(
            DataSourceFile.objects.filter(id=ds_file2.id).exists()
        )
        self.assertFalse(
            DataSourceFile.objects.filter(id=ds_file.id).exists()
        )


    @patch("core.utils.s3.remove_s3_folder_by_batch")
    @patch("gap.tasks.cleanup.timezone.now")
    def test_successful_cleanup_with_exc(
        self, mock_now, mock_remove_s3_folder
    ):
        """Test cleanup of old deleted Zarr files."""
        mock_now.return_value = datetime.datetime(
            2023, 10, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        ds_file = DataSourceFileFactory.create(
            name="test.zarr",
            format="ZARR",
            deleted_at=datetime.datetime(
                2023, 9, 18, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            is_latest=False,
            metadata={}
        )
        ds_file2 = DataSourceFileFactory.create(
            name="test2.zarr",
            format="ZARR",
            deleted_at=datetime.datetime(
                2023, 9, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            is_latest=False,
            metadata={}
        )

        mock_remove_s3_folder.side_effect = [
            {
                'total_deleted': 1,
                'total_batches': 1
            },
            Exception("S3 deletion failed")
        ]

        count, failed_count = cleanup_deleted_zarr()
        self.assertEqual(count, 1)
        self.assertEqual(failed_count, 1)
        self.assertEqual(mock_remove_s3_folder.call_count, 2)
        self.assertTrue(
            DataSourceFile.objects.filter(id=ds_file2.id).exists()
        )
        self.assertFalse(
            DataSourceFile.objects.filter(id=ds_file.id).exists()
        )
