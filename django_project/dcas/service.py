# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Service for Growth Stage
"""

from django.core.cache import cache
from dcas.models import GDDMatrix, DCASMessagePriority


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


class MessagePriorityService:
    """A service to manage message priorities."""

    CACHE_KEY = "message_priority:{code}:{config_id}"
    LOWEST_PRIORITY = 0

    @staticmethod
    def get_priority(code, config_id, bypass_db=False):
        """
        Get the message priority for a given code and config ID.

        :param code: Message code
        :type code: str
        :param config_id: Configuration ID
        :type config_id: int
        :param bypass_db: Flag to bypass database lookup
        :type bypass_db: bool
        :return: Priority value or None if not found
        :rtype: int or None
        """
        cache_key = MessagePriorityService.CACHE_KEY.format(
            code=code,
            config_id=config_id
        )
        result = cache.get(cache_key)
        if result is None:
            if bypass_db:
                # If bypass_db is True, return lowest priority
                return MessagePriorityService.LOWEST_PRIORITY

            # Load from the database if not in cache
            try:
                result = DCASMessagePriority.objects.get(
                    code=code,
                    config_id=config_id
                ).priority
                cache.set(cache_key, result, timeout=None)
            except DCASMessagePriority.DoesNotExist:
                # If not found in the database, return lowest priority
                return MessagePriorityService.LOWEST_PRIORITY
        return result

    @staticmethod
    def load_priority():
        """Preload all message priorities into the cache."""
        all_priorities = list(
            DCASMessagePriority.objects.all()
            .values("code", "config__id", "priority")
        )

        cache_map = {}
        for priority in all_priorities:
            cache_key = MessagePriorityService.CACHE_KEY.format(
                code=priority["code"],
                config_id=priority["config__id"]
            )
            cache_map[cache_key] = priority["priority"]

        # Efficient bulk cache set
        cache.set_many(cache_map, timeout=None)

    @staticmethod
    def cleanup_priority():
        """
        Remove all message priorities from the cache.

        Clears the cached message priorities,
        once the pipeline has completed.
        """
        all_cache_keys = [
            MessagePriorityService.CACHE_KEY.format(
                code=priority["code"],
                config_id=priority["config__id"]
            )
            for priority in DCASMessagePriority.objects.values(
                "code", "config__id"
            )
        ]

        if all_cache_keys:
            cache.delete_many(all_cache_keys)

    @staticmethod
    def sort_messages(messages, config_id, bypass_db=False):
        """
        Sort messages by priority in descending order.

        :param messages: List of message codes
        :type messages: list
        :param config_id: Configuration ID
        :type config_id: int
        :param bypass_db: Flag to bypass database lookup
        :type bypass_db: bool
        :return: Sorted list of messages by priority
        :rtype: list
        """
        sorted_messages = sorted(
            messages,
            key=(
                lambda x: MessagePriorityService.get_priority(
                    x, config_id, bypass_db
                )
            ),
            reverse=True
        )
        return sorted_messages
