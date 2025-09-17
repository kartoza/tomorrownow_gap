# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm SPW Generator.
"""

from datetime import datetime, timedelta, date

import pytz
from django.db import transaction
from django.utils import timezone

from core.utils.date import get_previous_day
from gap.models.crop_insight import (
    FarmSuitablePlantingWindowSignal, FarmShortTermForecast,
    FarmShortTermForecastData
)
from gap.models.farm_group import FarmGroup
from gap.models.farm import Farm
from spw.models import SPWErrorLog
from spw.generator.main import (
    calculate_from_point, calculate_from_grid, VAR_MAPPING_REVERSE,
    calculate_from_point_attrs
)
from spw.utils.plumber import PLUMBER_PORT


class CropInsightFarmGenerator:
    """Insight Farm Generator."""

    def __init__(self, farm: Farm, farm_group: FarmGroup, port=PLUMBER_PORT):
        """Init Generator."""
        self.farm = farm
        self.farm_group = farm_group
        self.today = timezone.now()
        self.today.replace(tzinfo=pytz.UTC)
        self.today = self.today.date()

        self.tomorrow = self.today + timedelta(days=1)
        self.attributes = calculate_from_point_attrs()
        self.port = port
        self.errors = []

        # Get config from farm group
        # This flag is used to enable the check for same tier 1 signal
        # from previous reference date
        self.check_same_tier_1_signal = self.farm_group.get_config(
            'check_same_tier_1_signal', False
        )
        ref_date_str = self.farm_group.get_config(
            'tier_1_reference_date', None
        )
        self.reference_date = (
            date.fromisoformat(ref_date_str) if ref_date_str else None
        )
        if self.reference_date:
            self.reference_date = get_previous_day(
                self.reference_date.weekday(), self.today
            )

    def return_float(self, value):
        """Return float value."""
        try:
            return float(value)
        except ValueError:
            return None

    def check_previous_tier_1_signal(self):
        """Check if previous tier 1 signal has been sent."""
        if not self.check_same_tier_1_signal or not self.reference_date:
            return False

        previous_signal = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm,
            generated_date=self.reference_date,
            signal__icontains="Tier 1"
        )

        return previous_signal.exists()

    def save_spw(
            self, farm: Farm, signal, too_wet_indicator, last_4_days,
            last_2_days, today_tomorrow, is_sent_before
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
                'today_tomorrow': self.return_float(today_tomorrow),
                'is_sent_before': is_sent_before,
                'prev_reference_date': self.reference_date

            }
        )

    def save_shortterm_forecast(self, historical_dict, farm: Farm):
        """Save spw data."""
        # Save the short term forecast
        c, is_created = FarmShortTermForecast.objects.get_or_create(
            farm=farm,
            forecast_date=self.today
        )
        if not is_created:
            # Delete the FarmShortTermForecastData
            FarmShortTermForecastData.objects.filter(
                forecast=c
            ).delete()

        # get the attributes
        attributes_dict = {}
        for k, v in VAR_MAPPING_REVERSE.items():
            attributes_dict[k] = self.attributes.filter(
                attribute__variable_name=v
            ).first()

        batch_insert = []
        # Save the short term forecast data
        for k, v in historical_dict.items():
            _date = datetime.strptime(v['date'], "%Y-%m-%d")
            _date = _date.replace(tzinfo=pytz.UTC)
            if self.tomorrow <= _date.date():
                for attr_name, val in v.items():
                    try:
                        attr = attributes_dict[attr_name]
                        if attr and val is not None:
                            batch_insert.append(
                                FarmShortTermForecastData(
                                    forecast=c,
                                    value_date=_date,
                                    dataset_attribute=attr,
                                    value=val
                                )
                            )
                    except KeyError:
                        pass
        # Save the batch insert
        if batch_insert:
            FarmShortTermForecastData.objects.bulk_create(
                batch_insert, ignore_conflicts=True
            )

    def generate_spw(self):
        """Generate spw.

        Do atomic because need all data to be saved.
        """
        try:
            with transaction.atomic():
                self._generate_spw()
        except Exception as e:
            print(f'Generate SPW Error: {str(e)}')
            raise e
        finally:
            # save error logs
            if self.errors:
                SPWErrorLog.objects.bulk_create(self.errors)
                self.errors = []

    def _generate_spw(self):
        """Generate Farm SPW."""
        # Check already being generated, no regenereated!
        if FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm,
            generated_date=self.today
        ).exists():
            return

        # Generate the spw
        generated = False
        retry = 1
        while not generated:
            try:
                if self.farm.grid:
                    output, historical_dict = calculate_from_grid(
                        self.farm.grid.geometry.centroid, port=self.port
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
                    self._add_error(
                        'SPW Error: {}'.format(str(e))
                    )
                    raise e
                retry += 1

        if output is None:
            self._add_error('SPW Output is empty')
            return

        # TODO:
        #  This will deprecated after we save shorterm
        #  forecast using Zarr per grid
        # Save to all farm that has same grid
        farms = [self.farm]
        if self.farm.grid:
            farms = self.farm_group.farms.filter(
                grid=self.farm.grid
            )

        # Check if previous Tier 1 signal exists
        tier_1_exists = self.check_previous_tier_1_signal()
        for farm in farms:
            self.save_spw(
                farm,
                output.data.goNoGo, output.data.tooWet, output.data.last4Days,
                output.data.last2Days, output.data.todayTomorrow, tier_1_exists
            )
            self.save_shortterm_forecast(historical_dict, farm)

    def _add_error(self, error):
        """Save error log of SPW."""
        self.errors.append(
            SPWErrorLog(
                farm=self.farm,
                farm_group=self.farm_group,
                grid_unique_id=(
                    self.farm.grid.unique_id if self.farm.grid else
                    None
                ),
                generated_date=self.today,
                error=error
            )
        )
