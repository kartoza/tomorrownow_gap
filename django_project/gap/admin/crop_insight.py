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
        generate_insight_report(query.id)
    modeladmin.message_user(
        request,
        'Process will be started in background!',
        messages.SUCCESS
    )


@admin.register(CropInsightRequest)
class CropInsightRequestAdmin(admin.ModelAdmin):
    """Admin for CropInsightRequest."""

    list_display = ('requested_date', 'farm_list', 'file_url')
    filter_horizontal = ('farms',)
    actions = (generate_insight_report_action,)
    readonly_fields = ('file',)

    def farm_list(self, obj: CropInsightRequest):
        """Return farm list."""
        return [farm.unique_id for farm in obj.farms.all()]

    def file(self, obj: CropInsightRequest):
        """Return file path."""
        return [farm.unique_id for farm in obj.farms.all()]

    def file_url(self, obj):
        """Return file url."""
        if obj.file:
            return format_html(
                f'<a href="{obj.file.url}" '
                f'target="__blank__">{obj.file.url}</a>'
            )
        return '-'
