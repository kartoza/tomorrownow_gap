# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: DCAS Rule Engine Variables
"""

import numpy as np


class DCASVariable:
    """Represent variables that are used in rule engine."""

    CROP = 'crop'
    PARAMETER = 'parameter'
    GROWTH_STAGE = 'growth_stage'
    VALUE = 'value'
    MESSAGE_CODE = 'message_code'


class DCASData:
    """Represent data that is used in rule engine."""

    def __init__(
        self, config_id, crop_id, stage_type_id, growth_stage_id, parameters
    ):
        """Initialize DCASData."""
        self.config_id = config_id
        self.crop_id = crop_id
        self.stage_type_id = stage_type_id
        self.growth_stage_id = growth_stage_id
        self.parameters = parameters
        self.message_codes = set()

    def add_message_code(self, code):
        """Append message code."""
        self.message_codes.add(code)

    @property
    def ruleset_key(self):
        """Get ruleset key for this data."""
        return f'{self.config_id}_{self.crop_id}_{self.stage_type_id}'

    @property
    def rule_data(self):
        """Get list of rule data."""
        return [
            {
                DCASVariable.PARAMETER: parameter['id'],
                DCASVariable.GROWTH_STAGE: self.growth_stage_id,
                DCASVariable.VALUE: self._normalize_value(parameter['value'])
            } for parameter in self.parameters
        ]

    def _normalize_value(self, val):
        if np.isnan(val):
            return 0
        if np.isinf(val):
            return 999999
        return val

    def is_empty(self):
        """Check if message_codes is empty."""
        return len(self.message_codes) == 0
