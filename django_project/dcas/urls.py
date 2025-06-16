# coding=utf-8
"""Tomorrow Now GAP – DCAS URL routes."""

from django.urls import path
from dcas.views import OutputDownloadView, OutputListView

app_name = "dcas"   # namespace for reverse("dcas:output-download", args=[pk])

urlpatterns = [
    # dcas/outputs/<pk>/download/
    path(
        "outputs/<int:pk>/download/",
        OutputDownloadView.as_view(),
        name="output-download",
    ),
    # list all recent outputs
    path(
        "outputs/",
        OutputListView.as_view(),
        name="output-list"
    ),
]
