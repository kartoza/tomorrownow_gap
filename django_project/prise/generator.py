# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: PRISE message generator.
"""

from typing import List
from datetime import datetime, date, time, timezone, timedelta

from gap.models.farm import Farm
from gap.models.farm_group import FarmGroup
from gap.models.pest import Pest
from prise.models.data import PriseData, PriseDataByPest
from prise.models.message import PriseMessage, PriseMessageSchedule
from prise.variables import PriseDataType, PriseMessageContextVariable


class PriseMessageContext:
    """Available ."""

    def __init__(
            self, farm: Farm, pest: Pest, message_group: str,
            generated_date: date):
        """Initialize PriseMessageContext."""
        self.farm = farm
        self.pest = pest
        self.message_group = message_group
        self.generated_date = generated_date
        self.data_type = (
            PriseDataType.get_type_by_message_group(message_group)
        )

    def _get_previous_month(self, dt: date):
        """Get previous month."""
        first = dt.replace(day=1)
        return first - timedelta(days=1)

    @property
    def context(self):
        """Return context to generate message.

        If context is empty dict, then no message should be generated.
        :return: Dictionary of context
        :rtype: dict
        """
        ctx = {}

        # get farm_group
        farm_group: FarmGroup = self.farm.farmgroup_set.first()
        if farm_group is None:
            return ctx

        ctx[PriseMessageContextVariable.FARM_GROUP_PHONE] = (
            farm_group.phone_number
        )

        # return if data_type is None / not TTA1 or TTA2 message
        if self.data_type is None:
            return ctx

        # get prise data
        prise_data = PriseData.objects.filter(
            farm=self.farm,
            data_type=self.data_type
        ).first()

        # if no available prise data, then we should not generate message
        if prise_data is None:
            return {}

        # get prise pest data
        pest_data = PriseDataByPest.objects.filter(
            data=prise_data,
            pest=self.pest
        ).first()
        if pest_data is None:
            return {}

        # check if pest_data.value is NaN or null
        if pest_data.value is None or str(pest_data.value).lower() == 'nan':
            return {}

        ctx[PriseMessageContextVariable.PLANTING_CURRENT_MONTH] = (
            self.generated_date.strftime('%B')
        )
        ctx[PriseMessageContextVariable.PLANTING_PREVIOUS_MONTH] = (
            self._get_previous_month(self.generated_date).strftime('%B')
        )
        ctx[PriseMessageContextVariable.PEST_WINDOW_DAY_1] = (
            int(pest_data.value) - 1
        )
        ctx[PriseMessageContextVariable.PEST_WINDOW_DAY_2] = (
            int(pest_data.value) + 1
        )

        return ctx


def generate_prise_message(
        farm: Farm, pest: Pest, generated_date: date) -> List[str]:
    """Generate prise message.

    :param farm: farm object
    :type farm: Farm
    :param pest: pest object
    :type pest: Pest
    :param generated_date: generated date
    :type generated_date: date
    :return: List of message
    :rtype: List[str]
    """
    result = []
    current_dt = datetime.combine(
        generated_date, time.min, timezone.utc
    )

    # fetch message schedule based on generated_date
    schedule = PriseMessageSchedule.get_schedule(current_dt)

    if schedule is None:
        return result

    # get context
    context = PriseMessageContext(
        farm, pest, schedule.group, generated_date).context

    if len(context) == 0:
        return result

    # get messages, use default template
    messages = PriseMessage.get_messages(pest, schedule.group, context)
    return [f'"{message}"' for message in messages]
