# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm SPW Generator.
"""

from datetime import datetime, timedelta

import pytz
from django.db import transaction
from django.utils import timezone

from gap.models.crop_insight import (
    FarmSuitablePlantingWindowSignal, FarmShortTermForecast,
    FarmShortTermForecastData
)
from gap.models.farm import Farm
from spw.generator.main import (
    calculate_from_point, calculate_from_polygon, VAR_MAPPING_REVERSE,
    calculate_from_point_attrs
)
from spw.utils.plumber import PLUMBER_PORT


class CropInsightFarmGenerator:
    """Insight Farm Generator."""

    def __init__(self, farm: Farm, port=PLUMBER_PORT):
        """Init Generator."""
        self.farm = farm
        self.today = timezone.now()
        self.today.replace(tzinfo=pytz.UTC)
        self.today = self.today.date()

        self.tomorrow = self.today + timedelta(days=1)
        self.attributes = calculate_from_point_attrs()
        self.port = port

    def return_float(self, value):
        """Return float value."""
        try:
            return float(value)
        except ValueError:
            return None

    def save_spw(
            self, farm: Farm, signal, too_wet_indicator, last_4_days,
            last_2_days, today_tomorrow
    ):
        """Save spw data."""
        # Save SPW
        FarmSuitablePlantingWindowSignal.objects.update_or_create(
            farm=farm,
            generated_date=self.today,
            defaults={
                'signal': signal,
                'too_wet_indicator': too_wet_indicator,
                'last_4_days': self.return_float(last_4_days),
                'last_2_days': self.return_float(last_2_days),
                'today_tomorrow': self.return_float(today_tomorrow)

            }
        )

    def save_shortterm_forecast(self, historical_dict, farm: Farm):
        """Save spw data."""
        # Save the short term forecast
        for k, v in historical_dict.items():
            _date = datetime.strptime(v['date'], "%Y-%m-%d")
            _date = _date.replace(tzinfo=pytz.UTC)
            if self.tomorrow <= _date.date():
                for attr_name, val in v.items():
                    try:
                        attr = self.attributes.filter(
                            attribute__variable_name=VAR_MAPPING_REVERSE[
                                attr_name
                            ]
                        ).first()
                        if attr:
                            c, _ = FarmShortTermForecast.objects.get_or_create(
                                farm=farm,
                                forecast_date=self.today
                            )
                            FarmShortTermForecastData.objects.update_or_create(
                                forecast=c,
                                value_date=_date,
                                dataset_attribute=attr,
                                defaults={
                                    'value': val
                                }
                            )
                    except KeyError:
                        pass

    def generate_spw(self):
        """Generate spw.

        Do atomic because need all data to be saved.
        """
        with transaction.atomic():
            self._generate_spw()

    def _generate_spw(self):
        """Generate Farm SPW."""
        # Check already being generated, no regenereated!
        if FarmSuitablePlantingWindowSignal.objects.filter(
                farm=self.farm,
                generated_date=self.today
        ).first():
            return

        # Generate the spw
        generated = False
        retry = 1
        while not generated:
            try:
                if self.farm.grid:
                    output, historical_dict = calculate_from_polygon(
                        self.farm.grid.geometry, port=self.port
                    )
                else:
                    output, historical_dict = calculate_from_point(
                        self.farm.geometry, port=self.port
                    )
                generated = True
            except Exception as e:
                # When error, retry until 3 times
                # If it is 3 times, raise the error
                if retry >= 3:
                    raise e
                retry += 1

        # TODO:
        #  This will deprecated after we save shorterm
        #  forecast using Zarr per grid
        # Save to all farm that has same grid
        farms = [self.farm]
        if self.farm.grid:
            farms = Farm.objects.filter(grid=self.farm.grid)

        for farm in farms:
            self.save_spw(
                farm,
                output.data.goNoGo, output.data.tooWet, output.data.last4Days,
                output.data.last2Days, output.data.todayTomorrow
            )
            self.save_shortterm_forecast(historical_dict, farm)
