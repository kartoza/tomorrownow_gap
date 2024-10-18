# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models

from core.models.common import Definition
from gap.models.common import Unit
from gap.models.dataset import Dataset
from gap.models.station import Station, StationHistory


class Attribute(Definition):
    """Model representing an attribute of a measurement."""

    variable_name = models.CharField(
        max_length=512
    )
    unit = models.ForeignKey(
        Unit, on_delete=models.CASCADE
    )
    is_active = models.BooleanField(
        default=True
    )


class DatasetAttribute(models.Model):
    """Model representing attribute that a dataset has."""

    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE
    )
    attribute = models.ForeignKey(
        Attribute, on_delete=models.CASCADE
    )
    source = models.CharField(
        max_length=512,
        help_text='Variable name in the source'
    )
    source_unit = models.ForeignKey(
        Unit, on_delete=models.CASCADE
    )
    ensembles = models.BooleanField(
        default=False,
        help_text=(
            'Flag indicating that the attribute is an array of 50 ensembles.'
        )
    )

    def __str__(self) -> str:
        return f'{self.attribute} - {self.dataset}'


class Measurement(models.Model):
    """
    Model representing a measurement taken at a station.

    Attributes:
        station (ForeignKey):
            Foreign key referencing the Station model based on station_id.
        dataset_attribute (ForeignKey):
            Foreign key referencing the DatasetAttribute model based on
            dataset_attribute_id.
        date_time (dateTime): Time of the measurement.
        value (float): Value of the measurement.
    """

    station = models.ForeignKey(
        Station, on_delete=models.CASCADE
    )
    dataset_attribute = models.ForeignKey(
        DatasetAttribute, on_delete=models.CASCADE
    )
    date_time = models.DateTimeField()
    value = models.FloatField()

    # Specifically measurement is linked to a station history
    station_history = models.ForeignKey(
        StationHistory, on_delete=models.CASCADE,
        help_text=(
            'Station history of the measurement.'
        ),
        null=True, blank=True
    )

    def __str__(self):
        return f'{self.date_time} - {self.value}'

    class Meta:  # noqa
        unique_together = ('station', 'dataset_attribute', 'date_time')
