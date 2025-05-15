# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for Tomorrow.io Ingestor.
"""


def mask_api_key_from_error(error_message: str) -> str:
    """Mask the API key in the error message."""
    if 'apikey' in error_message:
        # Mask the API key in the error message
        start_index = error_message.index('apikey=') + len('apikey=')
        end_index = (
            error_message.index('&', start_index) if
            '&' in error_message else len(error_message)
        )
        api_key = error_message[start_index:end_index]
        masked_api_key = '*' * len(api_key)
        return error_message.replace(api_key, masked_api_key)
    return error_message
