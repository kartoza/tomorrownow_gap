# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Custom JSON Encoder.
"""

import json
from datetime import datetime, date


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON Encoder to handle datetime and date objects."""

    def default(self, obj):
        """Override default method to handle datetime and date."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)
