# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor Tasks.
"""

from celery.utils.log import get_task_logger

from datetime import date
from django.core.mail import send_mail
from django.conf import settings
from django.utils import timezone

from core.celery import app
from core.utils.emails import get_admin_emails
from gap.models import (
    Dataset,
    DatasetStore,
    DataSourceFile,
    IngestorType,
    IngestorSession,
    IngestorSessionStatus,
    CollectorSession
)
from gap.tasks.ingestor import (
    run_ingestor_session
)
from gap.utils.ingestor_config import get_ingestor_config_from_preferences

logger = get_task_logger(__name__)


@app.task(name='collector_session')
def run_collector_session(_id: int):
    """Run collector."""
    session = None
    try:
        session = CollectorSession.objects.get(id=_id)
        session.run()
    except CollectorSession.DoesNotExist:
        logger.error(f"Collector Session {_id} does not exist")
        notify_collector_failure.delay(_id, "Collector session not found")
    except Exception as e:
        logger.error(f"Error in Collector Session {_id}: {str(e)}")
        notify_collector_failure.delay(_id, str(e))
    finally:
        if session and session.status == IngestorSessionStatus.FAILED:
            notify_collector_failure.delay(_id, session.notes)


@app.task(name='cbam_collector_session')
def run_cbam_collector_session():
    """Run Collector for CBAM Dataset."""
    session = CollectorSession.objects.create(
        ingestor_type=IngestorType.CBAM
    )
    session.run()
    if session.dataset_files.count() > 0:
        # create ingestor session to convert into zarr
        IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            collector=session
        )


def _do_run_zarr_collector(
    dataset: Dataset, collector_session: CollectorSession,
    ingestor_type
):
    """Run collector for zarr file.

    :param dataset: dataset
    :type dataset: Dataset
    :param collector_session: collector session to be run
    :type collector_session: CollectorSession
    :param ingestor_type: ingestor type
    :type ingestor_type: IngestorType
    """
    # run collector
    collector_session.run()

    # if success, create ingestor session
    collector_session.refresh_from_db()
    total_file = collector_session.dataset_files.count()
    if total_file > 0:
        additional_conf = {}
        config = get_ingestor_config_from_preferences(dataset.provider)
        # check ingestor retention policy for Hourly Tomorrow.io and Salient
        force_new_zarr = False
        if ingestor_type == IngestorType.HOURLY_TOMORROWIO:
            config = config.get('hourly_config', {})
            # Hourly will always write to new zarr file
            force_new_zarr = True
        elif ingestor_type == IngestorType.SALIENT:
            # Salient will write to new zarr file
            # if it's first week of the month
            if timezone.now().day <= 7:
                force_new_zarr = True

        if force_new_zarr:
            config['use_latest_datasource'] = False
            if 'datasourcefile_id' in config:
                # remove datasourcefile_id if exists
                del config['datasourcefile_id']
            if 'datasourcefile_exists' in config:
                # remove datasourcefile_exists if exists
                del config['datasourcefile_exists']

        use_latest_datasource = config.get('use_latest_datasource', True)
        if use_latest_datasource:
            # find latest DataSourceFile
            data_source = DataSourceFile.objects.filter(
                dataset=dataset,
                format=DatasetStore.ZARR,
                is_latest=True
            ).last()
            if data_source:
                additional_conf = {
                    'datasourcefile_id': data_source.id,
                    'datasourcefile_exists': True
                }
        additional_conf.update(config)

        # create session and trigger the task
        session = IngestorSession.objects.create(
            ingestor_type=ingestor_type,
            trigger_task=False,
            additional_config=additional_conf
        )
        session.collectors.add(collector_session)
        run_ingestor_session.delay(session.id)

    if collector_session.status == IngestorSessionStatus.FAILED:
        notify_collector_failure.delay(
            collector_session.id,
            collector_session.notes
        )


@app.task(name='salient_collector_session')
def run_salient_collector_session():
    """Run Collector for Salient Dataset."""
    dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
    collector_session = CollectorSession.objects.create(
        ingestor_type=IngestorType.SALIENT
    )
    _do_run_zarr_collector(dataset, collector_session, IngestorType.SALIENT)


@app.task(name='tio_collector_session')
def run_tio_collector_session():
    """Run Collector for Tomorrow.io Dataset."""
    dataset = Dataset.objects.get(
        name='Tomorrow.io Short-term Forecast',
        store_type=DatasetStore.ZARR
    )

    config = get_ingestor_config_from_preferences(dataset.provider)
    # create the collector object
    collector_session = CollectorSession.objects.create(
        ingestor_type=IngestorType.TIO_FORECAST_COLLECTOR,
        additional_config=config
    )
    _do_run_zarr_collector(dataset, collector_session, IngestorType.TOMORROWIO)


@app.task(name='tio_hourly_collector_session')
def run_tio_hourly_collector_session():
    """Run Collector for Hourly Tomorrow.io Dataset."""
    dataset = Dataset.objects.get(
        name='Tomorrow.io Short-term Hourly Forecast',
        store_type=DatasetStore.ZARR
    )

    config = get_ingestor_config_from_preferences(dataset.provider)
    hourly_config = config.get('hourly_config', {})
    # create the collector object
    collector_session = CollectorSession.objects.create(
        ingestor_type=IngestorType.HOURLY_TOMORROWIO,
        additional_config=hourly_config
    )
    _do_run_zarr_collector(
        dataset,
        collector_session,
        IngestorType.HOURLY_TOMORROWIO
    )


@app.task(name="notify_collector_failure")
def notify_collector_failure(session_id: int, exception: str):
    """
    Celery task to notify admins if a collector session fails.

    :param session_id: ID of the CollectorSession
    :param exception: Exception message describing the failure
    """
    # Retrieve the collector session
    session = None
    try:
        session = CollectorSession.objects.get(id=session_id)
        session.status = IngestorSessionStatus.FAILED  # Ensure correct status
        session.save()
    except CollectorSession.DoesNotExist:
        logger.warning(f"CollectorSession {session_id} not found.")
        return

    # Log failure (If needed, adjust this for collectors)
    logger.error(f"CollectorSession {session_id} failed: {exception}")

    # Send an email notification to admins
    admin_emails = get_admin_emails()

    if admin_emails:
        send_mail(
            subject="Collector Failure Alert",
            message=(
                f"Collector Session {session_id} - {session.ingestor_type} "
                "has failed.\n\n"
                f"Error: {exception}\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=admin_emails,
            fail_silently=False,
        )
        logger.info(f"Sent collector failure email to {admin_emails}")
    else:
        logger.warning("No admin email found.")

    return (
        f"Logged collector {session_id} failed. Admins notified."
    )


def check_date_in_collectors(date: date, collectors: list):
    """Check if date is in collectors."""
    for collector in collectors:
        forecast_date = collector.additional_config.get('forecast_date', None)
        if forecast_date and forecast_date == date.isoformat():
            total_file = collector.dataset_files.count()
            if (
                collector.status == IngestorSessionStatus.SUCCESS and
                total_file > 0
            ):
                return True, None
            else:
                return False, collector

    return False, None


def get_existing_collectors(collectors: list):
    """Get existing collectors."""
    existing_collectors = []
    for existing_collector in collectors:
        try:
            collector = CollectorSession.objects.get(
                id=existing_collector
            )
            existing_collectors.append(collector)
        except CollectorSession.DoesNotExist:
            logger.error(
                f"Existing collector {existing_collector} not found."
            )
    return existing_collectors


@app.task(name='salient_collector_historical')
def run_salient_collector_historical():
    """Run collector for historical salient dataset."""
    dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
    config = get_ingestor_config_from_preferences(dataset.provider)
    historical_task = config.get('historical_task', None)
    if historical_task is None:
        raise ValueError("No historical_task found in config.")

    collectors = []
    # add existing from config if any
    existing_collectors = historical_task.get('existing_collectors', None)
    if existing_collectors:
        collectors.extend(get_existing_collectors(existing_collectors))

    dates_to_collect = []
    # check if need to collect by year
    collect_by_year = historical_task.get('collect_by_year', None)
    # check if need to collect by list of date
    collect_by_dates = historical_task.get('collect_by_dates', None)
    if collect_by_year:
        # iterate each month in the year
        for month in range(1, 13):
            dates_to_collect.append(
                date(year=collect_by_year, month=month, day=1)
            )
    elif collect_by_dates:
        # iterate each date in the list
        for date_str in collect_by_dates:
            dates_to_collect.append(
                date.fromisoformat(date_str)
            )
    else:
        # default to use current date
        # NOTE: if using default, then should be run on first day of the month
        dates_to_collect.append(timezone.now().date())

    logger.info(
        f"Collecting historical salient dataset for dates: "
        f"{dates_to_collect}"
    )

    for forecast_date in dates_to_collect:
        exists, collector = check_date_in_collectors(
            forecast_date, collectors
        )
        if exists:
            logger.info(
                f"Collector for date {forecast_date} already exists."
            )
            continue

        # create the collector session
        exist_in_list = False
        if collector is None:
            collector = CollectorSession.objects.create(
                ingestor_type=IngestorType.SALIENT,
                additional_config={
                    'forecast_date': forecast_date.isoformat()
                }
            )
        else:
            exist_in_list = True

        # run the collector session
        collector.run()

        # if success, add to collectors
        collector.refresh_from_db()
        total_file = collector.dataset_files.count()
        if total_file > 0:
            if not exist_in_list:
                collectors.append(collector)
        else:
            logger.error(
                f"Collector for date {forecast_date} failed. "
                f"Please check the logs."
            )

    # check if successful,
    if len(collectors) == 0:
        raise ValueError(
            "No collector session created. Please check the logs."
        )

    logger.info(
        f"Creating ingestor session for historical salient dataset: "
        f"{len(collectors)} collectors - {dates_to_collect}"
    )
    # create the ingestor session
    session = IngestorSession.objects.create(
        ingestor_type=IngestorType.SALIENT,
        trigger_task=False,
        additional_config={
            'remove_temp_file': historical_task.get('remove_temp_file', True),
            'datasourcefile_name': (
                historical_task.get('datasourcefile_name', None)
            ),
            'datasourcefile_id': (
                historical_task.get('datasourcefile_id', None)
            ),
            'datasourcefile_exists': (
                historical_task.get('datasourcefile_exists', False)
            )
        }
    )
    session.collectors.set(collectors)
    session.save()
    run_ingestor_session.delay(session.id)


@app.task(name='salient_collector_monthly_historical')
def run_salient_collector_monthly_historical():
    """Run collector for monthly historical salient."""
    dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
    config = get_ingestor_config_from_preferences(dataset.provider)
    historical_task = config.get('historical_task', None)
    if historical_task is None:
        raise ValueError("No historical_task found in config.")

    # Date to collect is the first day of the last month
    today = timezone.now().date()
    first_day_this_month = today.replace(day=1)
    last_month = first_day_this_month - timezone.timedelta(days=1)
    forecast_date = last_month.replace(day=1)
    collector = CollectorSession.objects.create(
        ingestor_type=IngestorType.SALIENT,
        additional_config={
            'forecast_date': forecast_date.isoformat()
        }
    )

    # run the collector session
    collector.run()

    # if success, add to collectors
    collector.refresh_from_db()
    total_file = collector.dataset_files.count()
    if total_file == 0:
        raise RuntimeError(
            f"Collector for date {forecast_date} failed. "
            f"Please check the logs."
        )

    # create the ingestor session
    session = IngestorSession.objects.create(
        ingestor_type=IngestorType.SALIENT,
        trigger_task=False,
        additional_config={
            'remove_temp_file': historical_task.get('remove_temp_file', True),
            'datasourcefile_name': (
                historical_task.get('datasourcefile_name', None)
            ),
            'datasourcefile_id': (
                historical_task.get('datasourcefile_id', None)
            ),
            'datasourcefile_exists': (
                historical_task.get('datasourcefile_exists', False)
            )
        }
    )
    session.collectors.set([collector])
    session.save()
    run_ingestor_session.delay(session.id)
