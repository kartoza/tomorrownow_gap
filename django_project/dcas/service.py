# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Service for Growth Stage
"""

from django.core.cache import cache
from dcas.models import GDDMatrix


class GrowthStageService:
    """A service to manage growth stages."""

    CACHE_KEY = "gdd_matrix:{crop_id}:{crop_stage_type_id}:{config_id}"

    @staticmethod
    def get_growth_stage(crop_id, crop_stage_type_id, total_gdd, config_id):
        """
        Get the growth stage: crop ID, stage type, total GDD, config ID.

        The threshold value in the fixture is upper threshold.
        E.g. for Sorghum_Early:
        - Germination 0 to 60
        - Seeding and Establishment 61 to 400
        - Flowering 401 to 680
        This function will return the lower threshold to be used in
        identifying the start date, e.g. for Sorghum_Early:
        - Germination lower threshold 0
        - Seeding and Establishment lower threshold 60
        - Flowering lower threshold 400
        :param crop_id: ID of the crop
        :type crop_id: int
        :param crop_stage_type_id: ID of the crop stage type
        :type crop_stage_type_id: int
        :param total_gdd: Total accumulated GDD
        :type total_gdd: float
        :param config_id: ID of the configuration
        :type config_id: int
        :return: Dictionary containing growth id, label, and
            gdd_threshold (lower threshold)
        :rtype: dict or None
        """
        cache_key = GrowthStageService.CACHE_KEY.format(
            crop_id=crop_id,
            crop_stage_type_id=crop_stage_type_id,
            config_id=config_id
        )
        growth_stage_matrix = cache.get(cache_key)

        if growth_stage_matrix is None:
            # Load the matrix from the database if not in cache
            growth_stage_matrix = list(
                GDDMatrix.objects.filter(
                    crop_id=crop_id,
                    crop_stage_type_id=crop_stage_type_id,
                    config_id=config_id
                )
                .select_related("crop_growth_stage")
                .order_by("gdd_threshold")
                .values(
                    "gdd_threshold",
                    "crop_growth_stage__id",
                    "crop_growth_stage__name"
                )
            )
            if growth_stage_matrix:
                cache.set(cache_key, growth_stage_matrix, timeout=None)

        # Find the appropriate growth stage based on total GDD
        prev_stage = {
            'gdd_threshold': 0
        }
        for stage in growth_stage_matrix:
            if total_gdd <= stage["gdd_threshold"]:
                return {
                    "id": stage["crop_growth_stage__id"],
                    "label": stage["crop_growth_stage__name"],
                    "gdd_threshold": prev_stage["gdd_threshold"]
                }
            prev_stage = stage

        # Return the last stage if total GDD exceeds all thresholds
        # TODO: to be confirmed to return last stage or not
        if growth_stage_matrix:
            last_stage = growth_stage_matrix[-1]
            return {
                "id": last_stage["crop_growth_stage__id"],
                "label": last_stage["crop_growth_stage__name"],
                "gdd_threshold": last_stage["gdd_threshold"]
            }

        # No growth stage found
        return None

    @staticmethod
    def load_matrix():
        """Preload all GDD matrices into the cache."""
        all_matrices = list(
            GDDMatrix.objects.all()
            .select_related("crop_growth_stage")
            .values(
                "crop_id",
                "crop_stage_type_id",
                "config__id",
                "gdd_threshold",
                "crop_growth_stage__id",
                "crop_growth_stage__name"
            )
        )

        cache_map = {}
        for matrix in all_matrices:
            cache_key = GrowthStageService.CACHE_KEY.format(
                crop_id=matrix["crop_id"],
                crop_stage_type_id=matrix["crop_stage_type_id"],
                config_id=matrix["config__id"]
            )

            if cache_key not in cache_map:
                cache_map[cache_key] = []

            cache_map[cache_key].append({
                "gdd_threshold": matrix["gdd_threshold"],
                "crop_growth_stage__id": matrix["crop_growth_stage__id"],
                "crop_growth_stage__name": matrix["crop_growth_stage__name"]
            })

        # Sort growth stages by GDD threshold for each key
        for key in cache_map:
            cache_map[key] = sorted(
                cache_map[key],
                key=lambda x: x["gdd_threshold"]
            )

        # Efficient bulk cache set
        cache.set_many(cache_map, timeout=None)

    @staticmethod
    def cleanup_matrix():
        """
        Remove all GDD matrices from the cache.

        Clears the cached growth stage matrices,
        once the pipeline has completed.
        """
        all_cache_keys = [
            GrowthStageService.CACHE_KEY.format(
                crop_id=m["crop_id"],
                crop_stage_type_id=m["crop_stage_type_id"],
                config_id=m["config__id"]
            )
            for m in GDDMatrix.objects.values(
                "crop_id", "crop_stage_type_id", "config__id"
            )
        ]

        if all_cache_keys:
            cache.delete_many(all_cache_keys)
