# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Crop insight Tasks.
"""

from celery.utils.log import get_task_logger
from django.contrib.auth import get_user_model

from core.celery import app
from gap.models.crop_insight import CropInsightRequest
from gap.models.farm_group import FarmGroup

logger = get_task_logger(__name__)
User = get_user_model()


@app.task(name='generate_insight_report')
def generate_insight_report(_id: list):
    """Generate insight report."""
    request = CropInsightRequest.objects.get(id=_id)
    request.run()


@app.task(name="generate_crop_plan")
def generate_crop_plan():
    """Generate crop plan for registered farms."""
    # create report request
    user = User.objects.filter(is_superuser=True).first()
    for group in FarmGroup.objects.all():
        request = CropInsightRequest.objects.create(
            requested_by=user,
            farm_group=group,
        )
        generate_insight_report.delay(request.id)


@app.task(name="retry_crop_plan_generators")
def retry_crop_plan_generators():
    """Retry crop plan generator.

    This will run the crop plan generators but just run the is cancelled.
    If it already has spw data, it will also be skipped.
    """
    for request in CropInsightRequest.today_reports():
        request.run()
