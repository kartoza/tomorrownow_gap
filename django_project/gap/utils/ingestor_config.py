# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading ingestor config
"""


from gap.models import Provider, Preferences


def get_ingestor_config_from_preferences(provider: Provider) -> dict:
    """Retrieve additional config for a provider.

    :param provider: provider
    :type provider: Provider
    :return: additional config for Ingestor
    :rtype: dict
    """
    config = Preferences.load().ingestor_config
    return config.get(provider.name, {})
