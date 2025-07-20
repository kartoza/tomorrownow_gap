# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Exceptions for ingestor.
"""


class FileNotFoundException(Exception):
    """File not found."""

    def __init__(self):  # noqa
        self.message = 'File not found.'
        super().__init__(self.message)


class FileIsNotCorrectException(Exception):
    """File not found."""

    def __init__(self, message):  # noqa
        super().__init__(message)


class ApiKeyNotFoundException(Exception):
    """Api key not found."""

    def __init__(self):  # noqa
        self.message = 'Api key not found.'
        super().__init__(self.message)


class EnvIsNotSetException(Exception):
    """Env is not set error."""

    def __init__(self, ENV_KEY):  # noqa
        self.message = f'{ENV_KEY} is not set.'
        super().__init__(self.message)


class AdditionalConfigNotFoundException(Exception):
    """Error when additional config is not found."""

    def __init__(self, key):  # noqa
        self.message = f'{key} is required in additional_config.'
        super().__init__(self.message)


class MissingCollectorSessionException(Exception):
    """Collector session not found."""

    def __init__(self, session_id):  # noqa
        self.message = (
            f'Missing collector session in IngestorSession {session_id}.'
        )
        super().__init__(self.message)


class NoDataException(Exception):
    """No data found."""

    def __init__(self, date, data_type):  # noqa
        self.message = (
            f'No data found for {data_type} on {date}.'
        )
        super().__init__(self.message)
