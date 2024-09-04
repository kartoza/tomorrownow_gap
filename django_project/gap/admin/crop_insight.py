# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin, messages
from django.utils.html import format_html

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Crop, Pest,
    FarmShortTermForecast, FarmShortTermForecastData,
    FarmProbabilisticWeatherForcast,
    FarmSuitablePlantingWindowSignal, FarmPlantingWindowTable,
    FarmPestManagement, FarmCropVariety, CropInsightRequest
)
from gap.tasks.crop_insight import generate_insight_report


@admin.register(Crop)
class CropAdmin(AbstractDefinitionAdmin):
    """Crop admin."""

    pass


@admin.register(Pest)
class PestAdmin(AbstractDefinitionAdmin):
    """Pest admin."""

    pass


class FarmShortTermForecastDataInline(admin.TabularInline):
    """FarmShortTermForecastData inline."""

    model = FarmShortTermForecastData
    extra = 0


@admin.register(FarmShortTermForecast)
class FarmShortTermForecastAdmin(admin.ModelAdmin):
    """Admin for FarmShortTermForecast."""

    list_display = (
        'farm', 'forecast_date'
    )
    filter = ('farm', 'forecast_date')
    inlines = (FarmShortTermForecastDataInline,)


@admin.register(FarmProbabilisticWeatherForcast)
class FarmProbabilisticWeatherForcastAdmin(admin.ModelAdmin):
    """Admin for FarmProbabilisticWeatherForcast."""

    list_display = (
        'farm', 'forecast_date', 'forecast_period'
    )
    filter = ('farm', 'forecast_date')


@admin.register(FarmSuitablePlantingWindowSignal)
class FarmSuitablePlantingWindowSignalAdmin(admin.ModelAdmin):
    """Admin for FarmSuitablePlantingWindowSignal."""

    list_display = (
        'farm', 'generated_date', 'signal'
    )
    filter = ('farm', 'generated_date')


@admin.register(FarmPlantingWindowTable)
class FarmPlantingWindowTableAdmin(admin.ModelAdmin):
    """Admin for FarmPlantingWindowTable."""

    list_display = (
        'farm', 'recommendation_date', 'recommended_date'
    )
    filter = ('farm', 'recommendation_date')


@admin.register(FarmPestManagement)
class FarmPestManagementAdmin(admin.ModelAdmin):
    """Admin for FarmPestManagement."""

    list_display = (
        'farm', 'recommendation_date', 'spray_recommendation'
    )
    filter = ('farm', 'recommendation_date')


@admin.register(FarmCropVariety)
class FarmCropVarietyAdmin(admin.ModelAdmin):
    """Admin for FarmCropVariety."""

    list_display = (
        'farm', 'recommendation_date', 'recommended_crop'
    )
    filter = ('farm', 'recommendation_date')


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
        'requested_date', 'farm_count', 'file_url', 'last_task_status',
        'background_tasks'
    )
    filter_horizontal = ('farms',)
    actions = (generate_insight_report_action,)
    readonly_fields = ('file',)

    def farm_count(self, obj: CropInsightRequest):
        """Return farm list."""
        return obj.farms.count()

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
