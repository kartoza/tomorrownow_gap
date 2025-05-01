# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Data Type for DCAS variables
"""


class DCASDataVariable:
    """String for DCAS variables."""

    CROP_ID = 'crop_id'
    CROP_STAGE_TYPE_ID = 'crop_stage_type_id'
    GROWTH_STAGE_ID = 'growth_stage_id'
    PREV_GROWTH_STAGE_ID = 'prev_growth_stage_id'
    GRID_ID = 'grid_id'
    PLANTING_DATE_EPOCH = 'planting_date_epoch'
    GROWTH_STAGE_START_DATE_EPOCH = 'growth_stage_start_date'
    PREV_GROWTH_STAGE_START_DATE_EPOCH = 'prev_growth_stage_start_date'
    GRID_CROP_KEY = 'grid_crop_key'
    CONFIG_ID = 'config_id'
    TOTAL_GDD = 'total_gdd'
    SEASONAL_PRECIPITATION = 'seasonal_precipitation'
    TEMPERATURE = 'temperature'
    HUMIDITY = 'humidity'
    P_PET = 'p_pet'
    GROWTH_STAGE_PRECIPITATION = 'growth_stage_precipitation'
    MESSAGE = 'message'
    MESSAGE_2 = 'message_2'
    MESSAGE_3 = 'message_3'
    MESSAGE_4 = 'message_4'
    MESSAGE_5 = 'message_5'
    FINAL_MESSAGE = 'final_message'
    PREV_WEEK_MESSAGE = 'prev_week_message'
    IS_EMPTY_MESSAGE = 'is_empty_message'
    HAS_REPETITIVE_MESSAGE = 'has_repetitive_message'
    GROUP_ID = 'group_id'
    FARM_ID = 'farm_id'
    FARM_UNIQUE_ID = 'farm_unique_id'
    GRID_UNIQUE_ID = 'grid_unique_id'
    FARM_REGISTRY_ID = 'registry_id'
    GEOMETRY = 'geometry'
    CROP = 'crop'
    ISO_A3 = 'iso_a3'
    COUNTRY_ID = 'country_id'
    COUNTY = 'county'
    SUBCOUNTY = 'subcounty'
    WARD = 'ward'
    PREFERRED_LANGUAGE = 'preferred_language'
    DATE = 'date'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    GROWTH_STAGE = 'growth_stage'


class DCASDataType:
    """Pandas type for DCAS variable."""

    MAP_TYPES = {
        DCASDataVariable.CROP_ID: 'UInt16',
        DCASDataVariable.CROP_STAGE_TYPE_ID: 'UInt16',
        DCASDataVariable.GROWTH_STAGE_ID: 'UInt16',
        DCASDataVariable.PREV_GROWTH_STAGE_ID: 'UInt16',
        DCASDataVariable.GRID_ID: 'UInt32',
        DCASDataVariable.PLANTING_DATE_EPOCH: 'UInt32',
        DCASDataVariable.GROWTH_STAGE_START_DATE_EPOCH: 'UInt32',
        DCASDataVariable.PREV_GROWTH_STAGE_START_DATE_EPOCH: 'UInt32',
        DCASDataVariable.CONFIG_ID: 'UInt16',
        DCASDataVariable.MESSAGE: 'UInt32',
        DCASDataVariable.MESSAGE_2: 'UInt32',
        DCASDataVariable.MESSAGE_3: 'UInt32',
        DCASDataVariable.MESSAGE_4: 'UInt32',
        DCASDataVariable.MESSAGE_5: 'UInt32',
        DCASDataVariable.FINAL_MESSAGE: 'UInt32',
        DCASDataVariable.PREV_WEEK_MESSAGE: 'UInt32',
        DCASDataVariable.IS_EMPTY_MESSAGE: 'bool',
        DCASDataVariable.HAS_REPETITIVE_MESSAGE: 'bool',
        DCASDataVariable.GROUP_ID: 'UInt32',
        DCASDataVariable.FARM_ID: 'Int64',
        DCASDataVariable.FARM_REGISTRY_ID: 'Int64',
        DCASDataVariable.COUNTRY_ID: 'UInt32',
        DCASDataVariable.YEAR: 'UInt16',
        DCASDataVariable.MONTH: 'UInt16',
        DCASDataVariable.DAY: 'UInt16',
    }

    @classmethod
    def get_column_map(cls, column_list):
        """Get the column map for the given column list.

        :param column_list: List of columns
        :type column_list: list
        :return: Dictionary of columns and their types
        :rtype: dict
        """
        return {
            col: cls.MAP_TYPES[col] for col in column_list if \
                col in cls.MAP_TYPES
        }
