# coding=utf-8
"""
Tomorrow Now GAP DCAS API View.

.. note:: API View for DCAS
"""

from datetime import timedelta

from django.conf import settings
from django.utils import timezone
from django.core.files.storage import storages
from rest_framework import generics, permissions
from rest_framework.views import APIView
from rest_framework.response import Response

from gap.models import Country, Preferences
from frontend.permissions import IsKalroUser
from dcas.models.output import DCASOutput
from dcas.models.download_log import DCASDownloadLog
from dcas.models.permission import DCASPermissionType
from frontend.serializers import OutputSerializer


class OutputListView(generics.ListAPIView):
    """Return recent CSV outputs (last 2 weeks) for KALRO users/admins."""

    permission_classes = [permissions.IsAuthenticated, IsKalroUser]
    serializer_class = OutputSerializer
    pagination_class = None  # client-side pagination in React

    def get_queryset(self):
        """Return queryset of recent DCAS outputs."""
        cutoff = timezone.now() - timedelta(weeks=2)
        preferences = Preferences.load()
        user = self.request.user
        countries = preferences.dcas_config.get(
            'countries',
            {}
        ).keys()
        permitted_countries = []
        country_objs = Country.objects.filter(name__in=countries)
        if not user.is_superuser:
            for country_obj in country_objs:
                has_perm = (
                    self.request.user.has_perm(
                        DCASPermissionType.VIEW_DCAS_OUTPUT_COUNTRY,
                        country_obj
                    )
                )
                if has_perm:
                    permitted_countries.append(country_obj)
        else:
            permitted_countries = list(country_objs)

        # find list of Country that the user has permission to view
        return (
            DCASOutput.objects.filter(
                delivered_at__gte=cutoff,
                request__country__in=permitted_countries
            ).order_by("-delivered_at")
        )


class OutputDownloadView(APIView):
    """View to generate presigned URL for downloading DCAS output files."""

    permission_classes = [permissions.IsAuthenticated, IsKalroUser]

    def get(self, request, pk: int, *args, **kwargs):
        """Generate a presigned URL for downloading a DCAS output file."""
        output = generics.get_object_or_404(DCASOutput, pk=pk)
        storage = storages["gap_products"]

        presigned_url = storage.url(
            name=output.path,
            expire=900,
            parameters={
                "ResponseContentDisposition":
                f'attachment; filename="{output.file_name}"',
            },
        )
        presigned = presigned_url
        # **new** – persist audit row
        DCASDownloadLog.objects.create(output=output, user=request.user)
        if settings.DEBUG:
            presigned = presigned_url.replace(
                "http://minio:9000", "http://localhost:9010"
            )

        return Response({"url": presigned})
