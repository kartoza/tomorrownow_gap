# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: DCAS Rule Engine Variables
"""



class DCASVariable:
    """Represent variables that are used in rule engine."""

    CROP = 'crop'
    PARAMETER = 'parameter'
    GROWTH_STAGE = 'growth_stage'
    VALUE = 'value'
    MESSAGE_CODE = 'message_code'


class DCASData:
    """Represent data that is used in rule engine."""

    def __init__(self, crop_id, stage_type_id, growth_stage_id, parameters):
        """Initialize DCASData."""
        self.crop_id = crop_id
        self.stage_type_id = stage_type_id
        self.growth_stage_id = growth_stage_id
        self.parameters = parameters
        self.message_codes = []

    def add_message_code(self, code):
        """Append message code."""
        if code not in self.message_codes:
            self.message_codes.append(code)

    @property
    def ruleset_key(self):
        """Get ruleset key for this data."""
        return f'{self.crop_id}_{self.stage_type_id}'

    @property
    def rule_data(self):
        """Get list of rule data."""
        return [
            {
                DCASVariable.PARAMETER: parameter['name'],
                DCASVariable.GROWTH_STAGE: self.growth_stage_id,
                DCASVariable.VALUE: parameter['value']
            } for parameter in self.parameters
        ]
