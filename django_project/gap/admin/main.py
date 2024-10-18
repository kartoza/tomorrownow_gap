# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
import os
import shutil

from django.contrib import admin, messages

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Attribute, Country, Provider, Measurement, IngestorSession,
    IngestorSessionProgress, Dataset, DatasetAttribute, DataSourceFile,
    DatasetType, Unit, Village, CollectorSession, DatasetStore,
    DataSourceFileCache
)
from gap.tasks.collector import run_collector_session
from gap.tasks.ingestor import run_ingestor_session
from gap.utils.zarr import BaseZarrReader


@admin.register(Unit)
class UnitAdmin(AbstractDefinitionAdmin):
    """Unit admin."""

    pass


@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    """Attribute admin."""

    list_display = (
        'name', 'description', 'variable_name', 'unit',
    )
    search_fields = ('name',)


@admin.register(Country)
class CountryAdmin(AbstractDefinitionAdmin):
    """Country admin."""

    pass


@admin.register(Provider)
class ProviderAdmin(AbstractDefinitionAdmin):
    """Provider admin."""

    pass


@admin.register(DatasetType)
class DatasetTypeAdmin(admin.ModelAdmin):
    """DatasetType admin."""

    list_display = (
        'name', 'type'
    )


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    """Dataset admin."""

    list_display = (
        'name', 'provider', 'type', 'time_step',
        'store_type', 'is_internal_use'
    )


@admin.register(DatasetAttribute)
class DatasetAttributeAdmin(admin.ModelAdmin):
    """DatasetAttribute admin."""

    list_display = (
        'dataset', 'attribute', 'source', 'source_unit',
    )
    list_filter = ('dataset',)


@admin.register(Measurement)
class MeasurementAdmin(admin.ModelAdmin):
    """Measurement admin."""

    list_display = (
        'station', 'dataset_attribute', 'date_time', 'value', 'station_history'
    )
    list_filter = ('station',)
    search_fields = ('name',)


class IngestorSessionProgressInline(admin.TabularInline):
    """IngestorSessionProgress inline."""

    model = IngestorSessionProgress
    extra = 0


@admin.action(description='Run Ingestor Session Task')
def trigger_ingestor_session(modeladmin, request, queryset):
    """Run Ingestor Session."""
    for query in queryset:
        run_ingestor_session.delay(query.id)


@admin.register(IngestorSession)
class IngestorSessionAdmin(admin.ModelAdmin):
    """IngestorSession admin."""

    list_display = (
        'run_at', 'status', 'end_at', 'ingestor_type'
    )
    list_filter = ('ingestor_type', 'status')
    inlines = (IngestorSessionProgressInline,)
    actions = (trigger_ingestor_session,)


@admin.action(description='Run Collector Session Task')
def trigger_collector_session(modeladmin, request, queryset):
    """Run Collector Session."""
    for query in queryset:
        run_collector_session.delay(query.id)


@admin.register(CollectorSession)
class CollectorSessionAdmin(admin.ModelAdmin):
    """CollectorSession admin."""

    list_display = (
        'run_at', 'status', 'end_at', 'ingestor_type',
        'total_output'
    )
    list_filter = ('ingestor_type', 'status')
    actions = (trigger_collector_session,)

    def total_output(self, obj: CollectorSession):
        """Return total count."""
        return obj.dataset_files.count()


@admin.action(description='Load zarr cache')
def load_source_zarr_cache(modeladmin, request, queryset):
    """Load DataSourceFile zarr cache."""
    name = None
    for query in queryset:
        if query.format != DatasetStore.ZARR:
            continue
        name = query.name
        reader = BaseZarrReader(query.dataset, [], None, None, None)
        reader.setup_reader()
        reader.open_dataset(query)
        break
    if name is not None:
        modeladmin.message_user(
            request,
            f'{name} zarr cache has been loaded!',
            messages.SUCCESS
        )
    else:
        modeladmin.message_user(
            request,
            'Please select zarr data source!',
            messages.WARNING
        )


@admin.action(description='Clear zarr cache')
def clear_source_zarr_cache(modeladmin, request, queryset):
    """Clear DataSourceFile zarr cache."""
    name = None
    for query in queryset:
        if query.format != DatasetStore.ZARR:
            continue
        name = query.name
        zarr_path = BaseZarrReader.get_zarr_cache_dir(name)
        if os.path.exists(zarr_path):
            shutil.rmtree(zarr_path)
        break
    if name is not None:
        modeladmin.message_user(
            request,
            f'{zarr_path} has been cleared!',
            messages.SUCCESS
        )
    else:
        modeladmin.message_user(
            request,
            'Please select zarr data source!',
            messages.WARNING
        )


@admin.register(DataSourceFile)
class DataSourceFileAdmin(admin.ModelAdmin):
    """DataSourceFile admin."""

    list_display = (
        'name', 'dataset', 'format', 'start_date_time',
        'end_date_time', 'is_latest', 'created_on'
    )
    list_filter = ('dataset', 'format', 'is_latest')
    actions = (load_source_zarr_cache, clear_source_zarr_cache,)


@admin.register(DataSourceFileCache)
class DataSourceFileCacheAdmin(admin.ModelAdmin):
    """DataSourceFileCache admin."""

    list_display = (
        'get_name', 'get_dataset', 'hostname',
        'created_on', 'expired_on'
    )
    list_filter = ('hostname',)

    def get_name(self, obj: DataSourceFileCache):
        """Get name of data source.

        :param obj: data source object
        :type obj: DataSourceFileCache
        :return: name of the data source
        :rtype: str
        """
        return obj.source_file.name

    def get_dataset(self, obj: DataSourceFileCache):
        """Get dataset of data source.

        :param obj: data source object
        :type obj: DataSourceFileCache
        :return: dataset of the data source
        :rtype: str
        """
        return obj.source_file.dataset.name

    get_name.short_description = 'Name'
    get_name.admin_order_field = 'source_file__name'
    get_dataset.short_description = 'Dataset'
    get_dataset.admin_order_field = 'source_file__dataset__name'


@admin.register(Village)
class VillageAdmin(AbstractDefinitionAdmin):
    """Village admin."""

    pass
