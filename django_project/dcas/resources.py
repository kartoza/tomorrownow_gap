# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Resource class for DCASErrorLog
"""

from import_export.fields import Field
from import_export.resources import ModelResource
from dcas.models import DCASErrorLog


class DCASErrorLogResource(ModelResource):
    """Resource class for DCASErrorLog."""

    request_id = Field(
        attribute="request__id",
        column_name="Request ID"
    )
    error_type = Field(
        attribute="error_type",
        column_name="Error Type"
    )
    error_message = Field(
        attribute="error_message",
        column_name="Error Message"
    )
    logged_at = Field(
        attribute="logged_at",
        column_name="Logged At"
    )
    farm_unique_id = Field(
        column_name="Farm ID",
        attribute="farm__unique_id"
    )

    class Meta:
        """Meta class for DCASErrorLogResource."""

        model = DCASErrorLog
        fields = [
            "id", "request_id", "farm_unique_id",
            "error_type", "error_message", "logged_at"
        ]
        export_order = fields
