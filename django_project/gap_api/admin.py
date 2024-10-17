# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Admin for API Tracking
"""

import datetime
from django.contrib import admin
from django.http import JsonResponse
from django.db.models import Count
from django.db.models.functions import TruncDay
from rest_framework_tracking.admin import APIRequestLogAdmin
from rest_framework_tracking.models import APIRequestLog as BaseAPIRequestLog

from gap.models import DatasetType
from gap_api.models import APIRequestLog


admin.site.unregister(BaseAPIRequestLog)


class ProductTypeFilter(admin.SimpleListFilter):
    """Custom filter for product type field."""

    title = 'Product Type'
    parameter_name = 'product_type'

    def lookups(self, request, model_admin):
        """Get list of product type."""
        dataset_types = DatasetType.objects.exclude(
            variable_name='default'
        ).order_by('variable_name')
        return [(dt.variable_name, dt.variable_name) for dt in dataset_types]

    def queryset(self, request, queryset):
        """Filter queryset using product type."""
        if self.value():
            return queryset.filter(query_params__product=self.value())
        return queryset


class GapAPIRequestLogAdmin(APIRequestLogAdmin):
    """Admin class for APIRequestLog model."""

    list_display = (
        "id",
        "product_type",
        "requested_at",
        "response_ms",
        "status_code",
        "user",
        "view_method",
        "path",
        "remote_addr",
        "host",
    )
    list_filter = (ProductTypeFilter, "user", "status_code")
    search_fields = ()

    def product_type(self, obj: APIRequestLog):
        """Display product from query_params.

        :param obj: current row
        :type obj: APIRequestLog
        :return: product in json query_params
        :rtype: str
        """
        return obj.query_params.get('product', '-')

    product_type.short_description = 'Product Type'

    def changelist_view(self, request, extra_context=None):
        """Render the changelist view.

        :param request: request
        :type request: Request object
        :param extra_context: extra context, defaults to None
        :type extra_context: any, optional
        :return: Rendered view
        :rtype: any
        """
        # Aggregate api logs per day
        chart_data = self._generate_chart_data(request)

        extra_context = extra_context or {"chart_data": list(chart_data)}

        # Call the superclass changelist_view to render the page
        return super().changelist_view(request, extra_context=extra_context)

    def chart_data_endpoint(self, request):
        """Get response for chart data.

        :param request: request
        :type request: Request object
        :return: Chart data
        :rtype: JsonResponse
        """
        return JsonResponse(
            list(self._generate_chart_data(request)), safe=False)

    def _generate_chart_data(self, request):
        """Generate chart data and construct the filter from request object.

        :param request: request
        :type request: Request object
        :return: APIRequestLog group by Date and the count
        :rtype: list
        """
        start_date = request.GET.get("start_date", None)
        end_date = request.GET.get("end_date", None)
        product_type = request.GET.get("product_type", None)
        user_id = request.GET.get("user__id__exact", None)

        # convert start_date and end_date to datetime objects
        if start_date:
            start_date = datetime.datetime.strptime(
                start_date, "%Y-%m-%d").date()
        if end_date:
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        # handle requested_at__day, requested_at__month, requested_at__year
        other_filters = {}
        for key, val in request.GET.items():
            if key.startswith('requested_at__'):
                other_filters[key] = val

        return self._do_query_chart_data(
            start_date, end_date, product_type, user_id, other_filters)

    def _do_query_chart_data(
            self, start_date, end_date, product_type, user_id, other_filters):
        """Get chart data by filters.

        :param start_date: start date filter
        :type start_date: datetime
        :param end_date: end date filter
        :type end_date: datetime
        :param product_type: product type
        :type product_type: str
        :param user_id: user ID
        :type user_id: int
        :param other_filters: Dictionary of valid filter
        :type other_filters: dict
        :return: APIRequestLog group by Date and the count
        :rtype: list
        """
        filters = {}
        if start_date and end_date:
            filters['requested_at__date__gte'] = start_date
            filters['requested_at__date__lte'] = end_date

        if product_type:
            filters['query_params__product'] = product_type

        if user_id:
            filters['user__id'] = user_id

        filters.update(other_filters)
        return (
            APIRequestLog.objects.filter(
                **filters
            )
            .annotate(date=TruncDay("requested_at"))
            .values("date")
            .annotate(y=Count("id"))
            .order_by("-date")
        )


admin.site.register(APIRequestLog, GapAPIRequestLogAdmin)
