"""
Tomorrow Now GAP.

.. note:: DCAS Views
"""

from datetime import timedelta
from django.utils import timezone
from django.core.files.storage import storages
from rest_framework import generics, permissions
from rest_framework.views import APIView
from rest_framework.response import Response

from core.permissions import IsKalroUser
from dcas.models.output import DCASOutput
from dcas.models.download_log import DCASDownloadLog
from .serializers import OutputSerializer


class OutputListView(generics.ListAPIView):
    """Return recent CSV outputs (last 2 weeks) for KALRO users/admins."""

    permission_classes = [permissions.IsAuthenticated, IsKalroUser]
    serializer_class = OutputSerializer
    pagination_class = None  # client-side pagination in React

    def get_queryset(self):
        """Return queryset of recent DCAS outputs."""
        cutoff = timezone.now() - timedelta(weeks=2)
        return (
            DCASOutput.objects.filter(
                file_name__iendswith=".csv", delivered_at__gte=cutoff
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
                "response-content-disposition":
                f'attachment; filename="{output.file_name}"',
            },
        )

        # **new** – persist audit row
        DCASDownloadLog.objects.create(output=output, user=request.user)

        return Response({"url": presigned_url})
