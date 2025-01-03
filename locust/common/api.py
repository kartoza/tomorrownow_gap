# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: API Class for Locust Load Testing
"""


class ApiTaskTag:
    """Represent the tag for a task."""

    RANDOM_VAR = 'rand_var'
    RANDOM_OUTPUT = 'rand_out'
    RANDOM_DATE = 'rand_date'
    RANDOM_ALL = 'rand_all'


class ApiWeatherGroupMode:
    """Represents how to group the API requests."""

    BY_PRODUCT_TYPE = 1
    BY_OUTPUT_TYPE = 2
    BY_ATTRIBUTE_LENGTH = 3
    BY_DATE_COUNT = 4
    BY_QUERY_TYPE = 5

    @staticmethod
    def as_list():
        """Return the enum as list."""
        return [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE,
            ApiWeatherGroupMode.BY_OUTPUT_TYPE,
            ApiWeatherGroupMode.BY_ATTRIBUTE_LENGTH,
            ApiWeatherGroupMode.BY_DATE_COUNT,
            ApiWeatherGroupMode.BY_QUERY_TYPE,
        ]


class Api:
    """Provides api call to TNGAP."""

    def __init__(self, client, user):
        """Initialize the class."""
        self.client = client
        self.user = user

    def get_weather_request_name(
            self, group_modes, product_type, output_type, attributes,
            start_date, end_date, lat=None, lon=None, bbox=None,
            location_name=None, default_name=None):
        """Return request name."""
        names = []
        for mode in ApiWeatherGroupMode.as_list():
            if mode not in group_modes:
                continue

            name = ''
            if mode == ApiWeatherGroupMode.BY_PRODUCT_TYPE:
                name = product_type
            elif mode == ApiWeatherGroupMode.BY_OUTPUT_TYPE:
                name = output_type
            elif mode == ApiWeatherGroupMode.BY_ATTRIBUTE_LENGTH:
                name = f'ATTR{len(attributes)}'
            elif mode == ApiWeatherGroupMode.BY_DATE_COUNT:
                name = f'DT{(end_date - start_date).days}'
            elif mode == ApiWeatherGroupMode.BY_QUERY_TYPE:
                name = 'point'
                if bbox is not None:
                    name = 'bbox'
                elif location_name is not None:
                    name = 'loc'

            if name:
                names.append(name)

        return default_name if len(names) == 0 else '_'.join(names)

    def weather(
        self, product_type, output_type, attributes, start_date, end_date,
        lat=None, lon=None, bbox=None, location_name=None, group_modes=None
    ):
        """Call weather API."""
        if group_modes is None:
            group_modes = [
                ApiWeatherGroupMode.BY_PRODUCT_TYPE,
                ApiWeatherGroupMode.BY_OUTPUT_TYPE
            ]
        request_name = self.get_weather_request_name(
            group_modes, product_type, output_type, attributes,
            start_date, end_date, lat=lat, lon=lon, bbox=bbox,
            location_name=location_name, default_name='weather'
        )
        attributes_str = ','.join(attributes)
        url = (
            f'/api/v1/measurement/?attributes={attributes_str}&' +
            f'start_date={start_date}&end_date={end_date}&' +
            f'product={product_type}&output_type={output_type}'
        )

        if location_name:
            url += f'&location_name={location_name}'
        elif bbox:
            url += f'&bbox={bbox}'
        else:
            url += f'&lat={lat}&lon={lon}'

        headers = {
            'Authorization': self.user['auth'],
            'user-agent': (
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
                '(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
            )
        }

        self.client.get(url, headers=headers, name=request_name)
