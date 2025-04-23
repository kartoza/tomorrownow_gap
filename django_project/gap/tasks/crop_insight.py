# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Crop insight Tasks.
"""

from celery.utils.log import get_task_logger
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.conf import settings

from core.celery import app
from core.utils.emails import get_admin_emails
from gap.models.crop_insight import CropInsightRequest
from gap.models.farm_group import FarmGroup

logger = get_task_logger(__name__)
User = get_user_model()


@app.task(name="notify_spw_error")
def notify_spw_error(date, farm_group_id, error_message):
    """Notify spw error to the user."""
    farm_group = FarmGroup.objects.get(id=farm_group_id)
    # Send an email notification to admins
    admin_emails = get_admin_emails()
    if admin_emails:
        send_mail(
            subject="SPW Failure Alert",
            message=(
                f"SPW for {farm_group.name} - {date} "
                "has failed.\n\n"
                f"Error: {error_message}\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=admin_emails,
            fail_silently=False,
        )
        logger.info(f"Sent SPW failure email to {admin_emails}")
    else:
        logger.warning("No admin email found.")


@app.task(name='generate_insight_report')
def generate_insight_report(_id: list):
    """Generate insight report."""
    request = CropInsightRequest.objects.get(id=_id)
    error_message = None    
    try:        
        request.run()
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error generating insight report: {e}", exc_info=True)
        raise e
    finally:
        notify_spw_error.delay(
            request.requested_at.date(),
            request.farm_group.id,
            error_message
        )


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
