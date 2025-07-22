# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW fixtures.
"""


SPW_MESSAGE_DICT = {
    'Plant NOW Tier 1a': {
        'tier': '1a',
        'message': 'Plant Now',
        'description': 'Current forecast and historical rains indicate plant now.'
    },
    'Plant NOW Tier 1b': {
        'tier': '1b',
        'message': 'Plant Now',
        'description': 'Both current forecast historical rains have good signal to plant.'
    },
    'Plant NOW Tier 1c': {
        'tier': '1c',
        'message': 'Plant Now',
        'description': 'Historical rains signal time to plant and forecast has positive signal.'
    },
    'Plant NOW Tier 2': {
        'tier': '2',
        'message': 'Plant Now',
        'description': 'Weak planting signal - could plant some of your land.'
    },
    'Do NOT plant, WATCH Tier 3a': {
        'tier': '3a',
        'message': 'DO NOT PLANT',
        'description': 'Watch - there are some signals looking more positive for rains arrival.'
    },
    'Do NOT plant, WATCH Tier 3b': {
        'tier': '3b',
        'message': 'DO NOT PLANT',
        'description': 'Watch - some early signals that rains may be coming soon.'
    },
    'Do NOT plant, out of season rain Tier 4a': {
        'tier': '4a',
        'message': 'DO NOT PLANT',
        'description': 'Rain in forecast but history would suggest not yet time to plant.'
    },
    'Do NOT plant, DRY Tier 4b': {
        'tier': '4b',
        'message': 'DO NOT PLANT',
        'description': 'Wait for more positive forecast.'
    },
}
