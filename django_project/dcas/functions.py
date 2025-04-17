# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Functions to process row data.
"""

import pandas as pd

from dcas.rules.rule_engine import DCASRuleEngine
from dcas.rules.variables import DCASData
from dcas.service import GrowthStageService, MessagePriorityService
from dcas.utils import read_grid_crop_data


def calculate_growth_stage(
    row: pd.Series, epoch_list: list
) -> pd.Series:
    """Identify the growth stage and its start date.

    The calculation will be using GDD cumulative sum for each day.
    :param row: single row
    :type row: pd.Series
    :param epoch_list: list of processing date epoch
    :type epoch_list: list
    :return: row with growth_stage_id and growth_stage_start_date
    :rtype: pd.Series
    """
    # possible scenario:
    # - no prev_growth_stage_start_date or prev_growth_stage_id
    # - growth_stage_id is the same with prev_growth_stage_id
    # - growth_stage_id is different with prev_growth_stage_id
    #
    # for 2nd scenario, use the prev_growth_stage_start_date
    # for 1st and 3rd scenario, we need to find the date that
    # growth stage is changed

    # check cumulative GDD from last value
    growth_stage_dict = GrowthStageService.get_growth_stage(
        row['crop_id'],
        row['crop_stage_type_id'],
        row[f'gdd_sum_{epoch_list[-1]}'],
        row['config_id']
    )

    if growth_stage_dict is None:
        # no lookup value
        row['growth_stage_id'] = row['prev_growth_stage_id']
        row['growth_stage_start_date'] = row['prev_growth_stage_start_date']
        return row

    gdd_threshold = growth_stage_dict['gdd_threshold']
    row['growth_stage_id'] = growth_stage_dict['id']

    row['growth_stage_start_date'] = row['prev_growth_stage_start_date']
    if (
        not pd.isna(row['prev_growth_stage_id']) and
        row['growth_stage_id'] == row['prev_growth_stage_id']
    ):
        # the growth_stage_id is not changed
        return row

    if gdd_threshold == 0:
        # if threshold is 0, then we return the plantingDate
        row['growth_stage_start_date'] = row['planting_date_epoch']
        return row

    found = False
    prev_epoch = epoch_list[-1]
    for idx, epoch in reversed(list(enumerate(epoch_list))):
        if idx == len(epoch_list) - 1:
            # skip last item
            continue

        if epoch < row['planting_date_epoch']:
            row['growth_stage_start_date'] = row['planting_date_epoch']
            found = True
            break

        sum_gdd = row[f'gdd_sum_{epoch}']

        if sum_gdd <= gdd_threshold:
            # found first sum_gdd that is
            # lesser than or equal to the lower threshold
            row['growth_stage_start_date'] = prev_epoch
            found = True
            break
        prev_epoch = epoch

    if not found:
        # if not found, then we assign the first epoch
        row['growth_stage_start_date'] = epoch_list[0]

    return row


def calculate_message_output(
    row: pd.Series, rule_engine: DCASRuleEngine, attrib_dict: dict
) -> pd.Series:
    """Execute rule engine and get the message output.

    :param row: single row
    :type row: pd.Series
    :param rule_engine: Rule Engine
    :type rule_engine: DCASRuleEngine
    :param attrib_dict: attribute and its id
    :type attrib_dict: dict
    :return: row with message output columns
    :rtype: pd.Series
    """
    params = [
        {
            'id': attribute_id,
            'value': row[key]
        } for key, attribute_id in attrib_dict.items()
    ]
    data = DCASData(
        row['config_id'],
        row['crop_id'],
        row['crop_stage_type_id'],
        row['growth_stage_id'],
        params
    )
    rule_engine.execute_rule(data)

    messages = MessagePriorityService.sort_messages(
        data.message_codes, row['config_id'], True
    )

    for idx, code in enumerate(messages):
        var_name = f'message_{idx + 1}' if idx > 0 else 'message'
        row[var_name] = code

    # Check if the message codes are empty
    if data.is_empty():
        # If empty, set is_empty_message flag to True
        row['is_empty_message'] = True
    else:
        row['is_empty_message'] = False
        # set the final message to be the highest priority message
        row['final_message'] = messages[0]
        if row['final_message'] == row['prev_week_message']:
            # set flag is_found_repetitive
            row['is_found_repetitive'] = True
            if len(messages) > 1:
                # set the final_message as the second message
                row['final_message'] = messages[1]

    return row


def get_last_message_dates(
    farm_id: int,
    min_allowed_date: pd.Timestamp,
    historical_parquet_path: str
) -> pd.DataFrame:
    """
    Get all messages for a given farm after min_allowed_date.

    :param farm_id: ID of the farm
    :type farm_id: int
    :param min_allowed_date: Minimum date for filtering messages
    :type min_allowed_date: pd.Timestamp
    :param historical_parquet_path: Path to historical message parquet file
    :type historical_parquet_path: str
    :return: Filtered DataFrame containing relevant messages
    :rtype: pd.DataFrame
    """
    # Read historical messages
    historical_data = read_grid_crop_data(historical_parquet_path, [], [])

    # Filter messages for the given farm and min_allowed_date
    filtered_data = historical_data[
        (historical_data['farm_id'] == farm_id) &
        (historical_data['message_date'] >= min_allowed_date)
    ].copy()

    return filtered_data


def get_last_message_date(
    farm_messages: pd.DataFrame,
    crop_id: int,
    message_code: str
) -> pd.Timestamp:
    """
    Get the last date a message code was sent for a specific crop.

    :param farm_messages: Pre-filtered messages for a farm
    :type farm_messages: pd.DataFrame
    :param crop_id: ID of the crop
    :type crop_id: int
    :param message_code: The message code to check
    :type message_code: str
    :return: Timestamp of the last message occurrence or None if not found
    :rtype: pd.Timestamp or None
    """
    # Further filter for the specific crop and message_code
    filtered_data = farm_messages[
        (farm_messages['crop_id'] == crop_id) &
        (
            (farm_messages['message'] == message_code) |
            (farm_messages['message_2'] == message_code) |
            (farm_messages['message_3'] == message_code) |
            (farm_messages['message_4'] == message_code) |
            (farm_messages['message_5'] == message_code)
        )
    ]

    if filtered_data.empty:
        return None

    # Return the most recent message date or None if empty
    return filtered_data['message_date'].max()


def filter_messages_by_weeks(
    df: pd.DataFrame,
    historical_parquet_path: str,
    weeks_constraint: int
) -> pd.DataFrame:
    """
    Remove messages that have been sent within the last X weeks.

    :param df: DataFrame containing new messages to be sent
    :type df: pd.DataFrame
    :param historical_parquet_path: Path to historical message parquet file
    :type historical_parquet_path: str
    :param weeks_constraint: Number of weeks to check for duplicate messages
    :type weeks_constraint: int
    :return: DataFrame with duplicate messages removed
    :rtype: pd.DataFrame
    """
    if 'farm_id' not in df.columns:
        df["farm_id"] = df["grid_id"]

    min_allowed_date = (
        pd.Timestamp.now() - pd.Timedelta(weeks=weeks_constraint)
    )

    # Load historical messages once for all farms in df
    unique_farm_ids = df['farm_id'].unique()
    historical_data = read_grid_crop_data(historical_parquet_path, [], [])

    # Filter historical messages for relevant farms and min_allowed_date
    historical_data = historical_data[
        (historical_data['farm_id'].isin(unique_farm_ids)) &
        (historical_data['message_date'] >= min_allowed_date)
    ]

    # Create a lookup dictionary
    message_lookup = {}
    for _, row in historical_data.iterrows():
        farm_id, crop_id = row['farm_id'], row['crop_id']
        for message_column in [
            'message',
            'message_2',
            'message_3',
            'message_4',
            'message_5'
        ]:
            message_code = row[message_column]
            if pd.notna(message_code):
                message_lookup[(farm_id, crop_id, message_code)] = max(
                    message_lookup.get(
                        (farm_id, crop_id, message_code), pd.Timestamp.min),
                    row['message_date']
                )

    # Remove messages that have already been sent recently
    for idx, row in df.iterrows():
        farm_id, crop_id = row['farm_id'], row['crop_id']
        for message_column in [
            'message',
            'message_2',
            'message_3',
            'message_4',
            'message_5'
        ]:
            message_code = row[message_column]

            if pd.isna(message_code):
                continue  # Skip empty messages

            last_sent_date = message_lookup.get(
                (farm_id, crop_id, message_code), None)

            if last_sent_date and last_sent_date >= min_allowed_date:
                df.at[idx, message_column] = None  # Remove duplicate message

    return df
