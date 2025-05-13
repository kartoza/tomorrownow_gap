# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Resource classes for Models
"""

from import_export.fields import Field
from import_export.resources import ModelResource
from gap_api.models import APIRequestLog


class APIRequestLogResource(ModelResource):
    """Resource class for APIRequestLog."""

    # Extracted fields for CSV export
    product = Field()
    attributes = Field()
    output_type = Field()
    start_date = Field()
    end_date = Field()

    class Meta:
        """Meta class for APIRequestLogResource."""

        model = APIRequestLog
        fields = [
            "id", "user__username", "query_params", "requested_at",
            "response_ms", "status_code", "view_method", "path",
            "remote_addr", "host",
            # Explicitly include custom extracted fields
            "product", "attributes", "output_type", "start_date", "end_date",
            "output_file_size"
        ]
        export_order = fields

    def _extract_from_query_params(self, obj, key):
        """Extract key from query_params."""
        if obj.query_params and isinstance(obj.query_params, dict):
            return obj.query_params.get(key, "")
        return ""

    def dehydrate_product(self, obj):
        """Dehydrate product field."""
        return self._extract_from_query_params(obj, "product")

    def dehydrate_attributes(self, obj):
        """Dehydrate attributes field."""
        return self._extract_from_query_params(obj, "attributes")

    def dehydrate_output_type(self, obj):
        """Dehydrate output_type field."""
        return self._extract_from_query_params(obj, "output_type")

    def dehydrate_start_date(self, obj):
        """Dehydrate start_date field."""
        return self._extract_from_query_params(obj, "start_date")

    def dehydrate_end_date(self, obj):
        """Dehydrate end_date field."""
        return self._extract_from_query_params(obj, "end_date")
