# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for generator functions.
"""

from datetime import datetime, date, time, timezone
from django.test import TestCase

from gap.models import FarmGroup, Pest
from gap.factories import (
    FarmFactory
)
from prise.models.data import (
    PriseData,
    PriseDataRawInput,
    PriseDataByPestRawInput
)
from prise.variables import (
    PriseDataType,
    PriseMessageGroup,
    PriseMessageContextVariable
)
from prise.generator import PriseMessageContext


class PriseGeneratorTest(TestCase):
    """Unit tests for Prise generator."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '10.pest.json',
        '11.farm_group.json',
        '1.messages.json',
        '1.prise_messages.json',
        '2.prise_pest.json',
    ]

    def setUp(self):
        """Set test class."""
        # create farm
        self.farm_1 = FarmFactory(unique_id='FARM1')
        self.farm_group = FarmGroup.objects.get(name='Regen organics pilot')
        self.pest = Pest.objects.get(name='Beanfly')
        self.date = date(2024, 10, 1)
        self.datetime = datetime.combine(
            self.date, time.min, timezone.utc
        )

    def test_get_type_by_message_group(self):
        """Test get_type_by_message_group."""
        type = PriseDataType.get_type_by_message_group(
            PriseMessageGroup.TIME_TO_ACTION_1)
        self.assertEqual(type, PriseDataType.CLIMATOLOGY)
        type = PriseDataType.get_type_by_message_group(
            PriseMessageGroup.TIME_TO_ACTION_2)
        self.assertEqual(type, PriseDataType.NEAR_REAL_TIME)
        type = PriseDataType.get_type_by_message_group(
            PriseMessageGroup.START_SEASON)
        self.assertFalse(type)

    def test_context_empty_farm_group(self):
        """Test context with empty farm group."""
        ctx = PriseMessageContext(
            self.farm_1, self.pest, PriseMessageGroup.START_SEASON,
            self.date)
        self.assertEqual(len(ctx.context), 0)

    def test_context_empty_data_type(self):
        """Test context with empty data type."""
        self.farm_group.farms.add(self.farm_1)
        ctx = PriseMessageContext(
            self.farm_1, self.pest, PriseMessageGroup.START_SEASON,
            self.date)
        self.assertEqual(len(ctx.context), 1)

    def test_context_empty_prise_data(self):
        """Test context with empty prise data."""
        self.farm_group.farms.add(self.farm_1)
        ctx = PriseMessageContext(
            self.farm_1, self.pest, PriseMessageGroup.TIME_TO_ACTION_1,
            self.date)
        self.assertEqual(len(ctx.context), 0)

    def test_context_empty_data(self):
        """Test context with empty data."""
        self.farm_group.farms.add(self.farm_1)
        PriseData.insert_data(
            PriseDataRawInput(
                farm_unique_id=self.farm_1.unique_id,
                generated_at=self.datetime,
                values=[],
                data_type=PriseDataType.CLIMATOLOGY
            )
        )
        ctx = PriseMessageContext(
            self.farm_1, self.pest, PriseMessageGroup.TIME_TO_ACTION_1,
            self.date)
        self.assertEqual(len(ctx.context), 0)

    def test_tta_context(self):
        """Test context for TTA message."""
        self.farm_group.farms.add(self.farm_1)
        PriseData.insert_data(
            PriseDataRawInput(
                farm_unique_id=self.farm_1.unique_id,
                generated_at=self.datetime,
                values=[
                    PriseDataByPestRawInput(
                        'ophiomyia_stat_v02_w1_nrt_midpoint',
                        10
                    )
                ],
                data_type=PriseDataType.CLIMATOLOGY
            )
        )
        ctx = PriseMessageContext(
            self.farm_1, self.pest, PriseMessageGroup.TIME_TO_ACTION_1,
            self.date)
        context = ctx.context
        self.assertEqual(len(context), 5)
        self.assertEqual(
            context[PriseMessageContextVariable.PEST_WINDOW_DAY_1],
            9
        )
        self.assertEqual(
            context[PriseMessageContextVariable.PEST_WINDOW_DAY_2],
            11
        )

    def test_context_nan_data(self):
        """Test context with NaN data."""
        self.farm_group.farms.add(self.farm_1)
        PriseData.insert_data(
            PriseDataRawInput(
                farm_unique_id=self.farm_1.unique_id,
                generated_at=self.datetime,
                values=[
                    PriseDataByPestRawInput(
                        'ophiomyia_stat_v02_w1_nrt_midpoint',
                        float('nan')
                    )
                ],
                data_type=PriseDataType.CLIMATOLOGY
            )
        )
        ctx = PriseMessageContext(
            self.farm_1, self.pest, PriseMessageGroup.TIME_TO_ACTION_1,
            self.date)
        context = ctx.context
        self.assertEqual(len(context), 0)
