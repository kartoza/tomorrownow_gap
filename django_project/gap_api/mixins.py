# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Mixins for API Tracking
"""

import json
from django.core.cache import cache
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework_tracking.base_mixins import BaseLoggingMixin


class GAPAPILoggingMixin(BaseLoggingMixin):
    """Mixin to log GAP API request."""

    CACHE_KEY = 'gap_logs_queue'

    def handle_log(self):
        """Store log to redis queue cache."""
        # remove data and response
        self.log['data'] = None
        self.log['response'] = None
        if self.log.get('user', None):
            # replace with user id
            self.log['user'] = self.log['user'].id
        cache._cache.get_client().rpush(
            self.CACHE_KEY,
            json.dumps(self.log, cls=DjangoJSONEncoder)
        )
