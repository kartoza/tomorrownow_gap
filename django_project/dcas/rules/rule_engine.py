# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: DCAS Rule Engine
"""

from durable.engine import MessageNotHandledException

from dcas.models import DCASConfig, DCASRule
from dcas.rules.host import DCASHost
from dcas.rules.variables import DCASData, DCASVariable


class DCASRuleEngine:
    """Class for DCAS Rule Engine."""

    def __init__(self, config: DCASConfig):
        """Initialize DCASRuleEngine."""
        self.config = config
        self.host = DCASHost()

    def initialize(self):
        """Load rule engine."""
        rules = DCASRule.objects.filter(
            config=self.config
        )

        ruleset_definitions = {}
        for idx, rule in enumerate(rules):
            ruleset_name = f'{rule.crop.id}_{rule.crop_stage_type.id}'
            rule_name = f'r_{idx}'
            condition = {
                'm': {
                    '$and': [
                        {
                            'parameter': rule.parameter.id
                        },
                        {
                            'growth_stage': rule.crop_growth_stage.id
                        },
                        {
                            '$gte': {
                                'value': rule.min_range
                            }
                        },
                        {
                            '$lt': {
                                'value': rule.max_range
                            }
                        }
                    ]
                }
            }

            if ruleset_name not in ruleset_definitions:
                ruleset_definitions[ruleset_name] = {}

            ruleset_definitions[ruleset_name][rule_name] = {
                'all': [condition],
                'run': rule.code
            }

        # register the ruleset
        self.host.register_rulesets(ruleset_definitions)

    def _execute(self, ruleset_name, item):
        message_code = None
        try:
            result = self.host.post(ruleset_name, item)
            message_code = result.get(DCASVariable.MESSAGE_CODE, None)
        except MessageNotHandledException:
            # no rule is found
            pass
        return message_code

    def execute_rule(self, data: DCASData):
        """Execute rule for given data.

        The message code will be added to DCASData.
        :param data: input data
        :type data: DCASData
        """
        for item in data.rule_data:
            message_code = self._execute(data.ruleset_key, item)

            if message_code:
                data.add_message_code(message_code)
