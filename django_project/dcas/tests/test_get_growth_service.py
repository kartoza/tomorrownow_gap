# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Test Service for Growth Stage
"""

from unittest.mock import patch
from django.test import TestCase
from dcas.models import GDDMatrix
from dcas.service import GrowthStageService


def set_cache_dummy(cache_key, growth_stage_matrix, timeout):
    """Set cache mock."""
    pass


class GrowthStageServiceTest(TestCase):
    """Test Growth Stage Service."""

    fixtures = [
        '1.object_storage_manager.json',
        '6.unit.json',
        '7.attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '1.dcas_config.json',
        '14.crop.json',
        '15.gdd_config.json',
        '16.gdd_matrix.json'
    ]

    @patch("dcas.service.cache")
    def test_get_growth_stage_from_cache(self, mock_cache):
        """Test retrieving growth stage from mocked cache."""
        # Mock cache.get to return a pre-cached value
        mock_cache.get.return_value = [
            {
                "gdd_threshold": 100,
                "crop_growth_stage__id": 1,
                "crop_growth_stage__name": "Germination",
                "config__id": 1
            },
            {
                "gdd_threshold": 150,
                "crop_growth_stage__id": 2,
                "crop_growth_stage__name": "Establishment",
                "config__id": 1
            },
        ]
        mock_cache.set.side_effect = set_cache_dummy

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 150, 1)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 2)
        self.assertEqual(stage["label"], "Establishment")
        self.assertEqual(stage["gdd_threshold"], 100)

        # Verify cache.get was called
        mock_cache.get.assert_called_once_with("gdd_matrix:2:1:1")

    @patch("dcas.service.cache")
    def test_get_growth_stage_database_fetch(self, mock_cache):
        """Test retrieving growth stage when cache is empty."""
        # Mock cache.get to return None
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy

        # Mock cache.set to simulate caching
        mock_cache.set = patch("dcas.service.cache.set").start()

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 150, 1)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 2)
        self.assertEqual(stage["label"], "Establishment")
        self.assertEqual(stage["gdd_threshold"], 105)

        # Verify cache.set was called to populate the cache
        mock_cache.set.assert_called_once()

    @patch("dcas.service.cache")
    def test_get_growth_stage_early_upper(self, mock_cache):
        """Test retrieving growth stage for high GDD values."""
        # Mock cache.get to return appropriate stage thresholds
        mock_cache.get.side_effect = lambda key: [
            {
                "gdd_threshold": 800,
                "crop_growth_stage__id": 7,
                "crop_growth_stage__name": "Grain Filling",
                "config__id": 1
            },
            {
                "gdd_threshold": 900,
                "crop_growth_stage__id": 8,
                "crop_growth_stage__name": "Physiological Maturity",
                "config__id": 1
            },
        ] if "8" in key else None
        mock_cache.set.side_effect = set_cache_dummy

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 1000, 1)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 8)
        self.assertEqual(stage["label"], "Physiological Maturity")
        self.assertEqual(stage["gdd_threshold"], 750)

        # Verify cache.get was called
        mock_cache.get.assert_called_with("gdd_matrix:2:1:1")

    @patch("dcas.service.cache")
    def test_get_growth_stage_below_threshold(self, mock_cache):
        """Test retrieving growth stage below all thresholds."""
        # Mock cache.get to return stages for crop 9, stage type 2
        mock_cache.get.return_value = [
            {
                "gdd_threshold": 100,
                "crop_growth_stage__id": 10,
                "crop_growth_stage__name": "Maturity",
                "config__id": 1
            },
            {
                "gdd_threshold": 200,
                "crop_growth_stage__id": 11,
                "crop_growth_stage__name": "Seedling",
                "config__id": 1
            },
        ]
        mock_cache.set.side_effect = set_cache_dummy

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(9, 2, 150, 1)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 11)
        self.assertEqual(stage["label"], "Seedling")
        self.assertEqual(stage["gdd_threshold"], 100)

        # Verify cache.get was called
        mock_cache.get.assert_called_once_with("gdd_matrix:9:2:1")

    @patch("dcas.service.cache")
    def test_get_growth_stage_no_matrix(self, mock_cache):
        """Test when no GDDMatrix exists for the crop and stage type."""
        # Mock cache.get to return None
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy

        # Ensure no matrix exists in the database
        GDDMatrix.objects.filter(crop_id=99, crop_stage_type_id=99).delete()

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(99, 99, 150, 99)
        self.assertIsNone(stage)

        # Verify cache.get was called
        mock_cache.get.assert_called_once_with("gdd_matrix:99:99:99")

    @patch("dcas.service.cache")
    @patch("dcas.models.GDDMatrix.objects")
    def test_load_matrix(self, mock_gdd_matrix_objects, mock_cache):
        """Test loading all GDD matrices into cache."""
        mock_gdd_matrices = [
            {
                "crop_id": 1,
                "crop_stage_type_id": 1,
                "config__id": 1,
                "gdd_threshold": 150,
                "crop_growth_stage__id": 2,
                "crop_growth_stage__name": "Establishment",
            },
            {
                "crop_id": 1,
                "crop_stage_type_id": 1,
                "config__id": 1,
                "gdd_threshold": 100,
                "crop_growth_stage__id": 1,
                "crop_growth_stage__name": "Germination",
            },
        ]

        mock_queryset = mock_gdd_matrix_objects.all.return_value
        mock_queryset.select_related.return_value = mock_queryset
        mock_queryset.values.return_value = mock_gdd_matrices

        GrowthStageService.load_matrix()

        expected_cache_data = {
            "gdd_matrix:1:1:1": [
                {
                    "gdd_threshold": 100,
                    "crop_growth_stage__id": 1,
                    "crop_growth_stage__name": "Germination",
                },
                {
                    "gdd_threshold": 150,
                    "crop_growth_stage__id": 2,
                    "crop_growth_stage__name": "Establishment",
                },
            ]
        }

        mock_cache.set_many.assert_called_once_with(
            expected_cache_data, timeout=None)

    @patch("dcas.service.cache")
    def test_cleanup_matrix(self, mock_cache):
        """Test cleaning up all GDD matrices from the cache."""
        mock_gdd_matrices = [
            {"crop_id": 1, "crop_stage_type_id": 1, "config__id": 1},
            {"crop_id": 2, "crop_stage_type_id": 1, "config__id": 2},
        ]

        with patch.object(
            GDDMatrix.objects,
            "values",
            return_value=mock_gdd_matrices
        ):
            GrowthStageService.cleanup_matrix()

        mock_cache.delete_many.assert_called_once_with([
            "gdd_matrix:1:1:1",
            "gdd_matrix:2:1:2",
        ])

    @patch("dcas.service.cache")
    def test_cleanup_matrix_no_keys(self, mock_cache):
        """Test cleaning up when no cached GDD matrices exist."""
        with patch.object(GDDMatrix.objects, "values", return_value=[]):
            GrowthStageService.cleanup_matrix()

        mock_cache.delete_many.assert_not_called()
