# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin, messages
from django.utils.html import format_html
from django_admin_inline_paginator.admin import TabularInlinePaginated

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Crop, FarmShortTermForecast, FarmShortTermForecastData,
    FarmProbabilisticWeatherForcast,
    FarmSuitablePlantingWindowSignal, FarmPlantingWindowTable,
    FarmPestManagement, FarmCropVariety, CropInsightRequest,
    CropStageType, CropGrowthStage
)
from gap.tasks.crop_insight import generate_insight_report
from spw.tasks import clean_duplicate_farm_short_term_forecast


@admin.register(Crop)
class CropAdmin(AbstractDefinitionAdmin):
    """Crop admin."""

    pass


class FarmShortTermForecastDataInline(TabularInlinePaginated):
    """FarmShortTermForecastData inline."""

    model = FarmShortTermForecastData
    per_page = 20
    extra = 0


@admin.action(description='Clean duplicate farm short term forecast')
def clean_duplicate_farm_short_term_forecast_action(
    modeladmin, request, queryset
):
    """Clean duplicate farm short term forecast."""
    clean_duplicate_farm_short_term_forecast.delay()
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.register(FarmShortTermForecast)
class FarmShortTermForecastAdmin(admin.ModelAdmin):
    """Admin for FarmShortTermForecast."""

    list_display = (
        'farm', 'forecast_date'
    )
    filter = ('forecast_date')
    readonly_fields = ('farm',)
    inlines = (FarmShortTermForecastDataInline,)
    actions = (clean_duplicate_farm_short_term_forecast_action,)


@admin.register(FarmProbabilisticWeatherForcast)
class FarmProbabilisticWeatherForcastAdmin(admin.ModelAdmin):
    """Admin for FarmProbabilisticWeatherForcast."""

    list_display = (
        'farm', 'forecast_date', 'forecast_period'
    )
    filter = ('forecast_date')
    readonly_fields = ('farm',)


@admin.register(FarmSuitablePlantingWindowSignal)
class FarmSuitablePlantingWindowSignalAdmin(admin.ModelAdmin):
    """Admin for FarmSuitablePlantingWindowSignal."""

    list_display = (
        'farm', 'generated_date', 'signal'
    )
    filter = ('generated_date')
    readonly_fields = ('farm',)


@admin.register(FarmPlantingWindowTable)
class FarmPlantingWindowTableAdmin(admin.ModelAdmin):
    """Admin for FarmPlantingWindowTable."""

    list_display = (
        'farm', 'recommendation_date', 'recommended_date'
    )
    filter = ('recommendation_date')
    readonly_fields = ('farm',)


@admin.register(FarmPestManagement)
class FarmPestManagementAdmin(admin.ModelAdmin):
    """Admin for FarmPestManagement."""

    list_display = (
        'farm', 'recommendation_date', 'spray_recommendation'
    )
    filter = ('recommendation_date')
    readonly_fields = ('farm',)


@admin.register(FarmCropVariety)
class FarmCropVarietyAdmin(admin.ModelAdmin):
    """Admin for FarmCropVariety."""

    list_display = (
        'farm', 'recommendation_date', 'recommended_crop'
    )
    filter = ('recommendation_date')
    readonly_fields = ('farm',)


@admin.action(description='Generate insight report')
def generate_insight_report_action(modeladmin, request, queryset):
    """Generate insight report."""
    for query in queryset:
        generate_insight_report.delay(query.id)
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.register(CropInsightRequest)
class CropInsightRequestAdmin(admin.ModelAdmin):
    """Admin for CropInsightRequest."""

    list_display = (
        'requested_at', 'farm_group', 'file_url',
        'last_task_status', 'background_tasks'
    )
    actions = (generate_insight_report_action,)
    readonly_fields = ('file',)

    def file_url(self, obj):
        """Return file url."""
        if obj.file:
            return format_html(
                f'<a href="{obj.file.url}" '
                f'target="__blank__">{obj.file.url}</a>'
            )
        return '-'

    def last_task_status(self, obj: CropInsightRequest):
        """Return task status."""
        bg_task = obj.last_background_task
        if bg_task:
            return bg_task.status
        return None

    def background_tasks(self, obj: CropInsightRequest):
        """Return ids of background tasks that are running."""
        url = (
            f"/admin/core/backgroundtask/?context_id__exact={obj.id}&"
            f"task_name__in={','.join(CropInsightRequest.task_names)}"
        )
        return format_html(f'<a target="_blank" href={url}>link</a>')


@admin.register(CropStageType)
class CropStageTypeAdmin(admin.ModelAdmin):
    """Admin for CropStageType."""

    pass


@admin.register(CropGrowthStage)
class CropGrowthStageAdmin(admin.ModelAdmin):
    """Admin for CropGrowthStage."""

    pass
