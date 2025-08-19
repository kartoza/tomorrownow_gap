# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Load fixtures
"""

import logging
import os
import re

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand

from core.settings.utils import DJANGO_ROOT

logger = logging.getLogger(__name__)


def extract_number(filename):
    """Sort by extracting the number at the beginning."""
    match = re.match(r'^(\d+)\.', filename)
    return int(match.group(1)) if match else 0


class Command(BaseCommand):
    """Command to load fixtures."""

    help = 'Load all fixtures'

    def handle(self, *args, **options):
        """Handle load fixtures."""
        apps = []
        for app in settings.PROJECT_APPS:
            if app != 'dcas':
                apps.append(app)
        # insert dcas after core
        for i, app in enumerate(apps):
            if app == 'core':
                apps.insert(i + 1, 'dcas')
                break
        print(apps)
        for app in apps:
            folder = os.path.join(
                DJANGO_ROOT, app, 'fixtures'
            )
            if os.path.exists(folder):
                for subdir, dirs, files in os.walk(folder):
                    sorted_files = sorted(files, key=extract_number)
                    print(sorted_files)
                    for file in sorted_files:
                        if file.endswith('.json'):
                            logger.info(f"Loading {file}")
                            print(f"Loading {app}/{file}")
                            call_command('loaddata', file)
