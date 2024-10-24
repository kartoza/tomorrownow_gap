# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: PRISE Config

    PRISE: a Pest Risk Information SErvice
    Pests can decimate crops and are estimated to cause around a 40% loss.
    These insects, mites and plant pathogens can impact on food security and
    impede supply chains and international trade.
    A Pest Risk Information SErvice (PRISE) aims to solve this problem by
    using data to help farmers manage pests in sub-Saharan Africa.
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class PriseConfig(AppConfig):
    """App Config for Prise."""

    name = 'prise'
    verbose_name = _('PRISE')

    def ready(self):
        """App ready handler."""
        pass
