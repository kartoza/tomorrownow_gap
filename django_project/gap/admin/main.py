# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
import os
import shutil

from django.contrib import admin, messages
from django_admin_inline_paginator.admin import TabularInlinePaginated

from core.admin import AbstractDefinitionAdmin
from core.utils.file import get_directory_size, format_size
from gap.models import (
    Attribute, Country, Provider, Measurement, IngestorSession,
    IngestorSessionProgress, Dataset, DatasetAttribute, DataSourceFile,
    DatasetType, Unit, Village, CollectorSession, DatasetStore,
    DataSourceFileCache
)
from gap.tasks.collector import run_collector_session
from gap.tasks.ingestor import (
    run_ingestor_session, convert_dataset_to_parquet
)
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


@admin.action(description='Run Parquet Converter Task')
def trigger_parquet_converter(modeladmin, request, queryset):
    """Run Parquet Converter."""
    for query in queryset:
        convert_dataset_to_parquet.delay(query.id)


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    """Dataset admin."""

    list_display = (
        'name', 'provider', 'type', 'time_step',
        'store_type', 'is_internal_use'
    )
    actions = (trigger_parquet_converter,)


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
    # fix N+1 issues for dataset_attribute dropdown
    readonly_fields = ('dataset_attribute', 'station_history',)

    def has_add_permission(self, request, obj=None):
        """Disable add measurement from admin page."""
        return False


class IngestorSessionProgressInline(TabularInlinePaginated):
    """IngestorSessionProgress inline."""

    model = IngestorSessionProgress
    per_page = 20
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


def _clear_zarr_cache(source: DataSourceFile, modeladmin, request):
    """Clear zarr cache directory."""
    if source.format != DatasetStore.ZARR:
        modeladmin.message_user(
            request,
            'Please select zarr data source!',
            messages.WARNING
        )
        return

    name = source.name
    zarr_path = BaseZarrReader.get_zarr_cache_dir(name)
    if os.path.exists(zarr_path):
        shutil.rmtree(zarr_path)

    modeladmin.message_user(
        request,
        f'{zarr_path} has been cleared!',
        messages.SUCCESS
    )

    # update cache size to 0
    DataSourceFileCache.objects.filter(
        source_file=source
    ).update(size=0)


@admin.action(description='Clear zarr cache')
def clear_source_zarr_cache(modeladmin, request, queryset):
    """Clear single DataSourceFile zarr cache."""
    for query in queryset:
        _clear_zarr_cache(query, modeladmin, request)
        break


@admin.register(DataSourceFile)
class DataSourceFileAdmin(admin.ModelAdmin):
    """DataSourceFile admin."""

    list_display = (
        'name', 'dataset', 'format', 'start_date_time',
        'end_date_time', 'is_latest', 'created_on'
    )
    list_filter = ('dataset', 'format', 'is_latest')
    actions = (load_source_zarr_cache, clear_source_zarr_cache,)


@admin.action(description='Calculate zarr cache size')
def calculate_zarr_cache_size(modeladmin, request, queryset):
    """Calculate the size of zarr cache."""
    for query in queryset:
        name = query.source_file.name
        zarr_path = BaseZarrReader.get_zarr_cache_dir(name)
        if not os.path.exists(zarr_path):
            continue

        query.size = get_directory_size(zarr_path)
        query.save()

    modeladmin.message_user(
        request,
        'Calculate zarr cache size successful!',
        messages.SUCCESS
    )


@admin.action(description='Clear zarr cache')
def clear_zarr_dir_cache(modeladmin, request, queryset):
    """Clear DataSourceFile zarr cache."""
    for query in queryset:
        _clear_zarr_cache(query.source_file, modeladmin, request)
        break


@admin.register(DataSourceFileCache)
class DataSourceFileCacheAdmin(admin.ModelAdmin):
    """DataSourceFileCache admin."""

    list_display = (
        'get_name', 'get_dataset', 'hostname',
        'get_cache_size', 'created_on', 'expired_on'
    )
    list_filter = ('hostname',)
    actions = (calculate_zarr_cache_size, clear_zarr_dir_cache,)

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

    def get_cache_size(self, obj: DataSourceFileCache):
        """Get the cache size."""
        return format_size(obj.size)

    get_name.short_description = 'Name'
    get_name.admin_order_field = 'source_file__name'
    get_dataset.short_description = 'Dataset'
    get_dataset.admin_order_field = 'source_file__dataset__name'
    get_cache_size.short_description = 'Cache Size'
    get_cache_size.admin_order_field = 'size'


@admin.register(Village)
class VillageAdmin(AbstractDefinitionAdmin):
    """Village admin."""

    pass
