# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: DCAS Rule Engine Host
"""

from durable import engine


class DCASHost(engine.Host):
    """Class to represent custom logic for durable engine host."""

    def __init__(self):
        """Initialize Custom Host for Rule Engine."""
        super(DCASHost, self).__init__()

    def get_action(self, action_name):
        """Set action_name (message_code) in rule engine state.

        :param action_name: message code
        :type action_name: str
        """
        def get_message_code(c):  # noqa
            c.s.message_code = action_name

        return get_message_code
