# coding=utf-8
"""
Tomorrow Now GAP – DCAS

Audit log capturing every CSV-download action.
"""

from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _

from dcas.models.output import DCASOutput


class DCASDownloadLog(models.Model):
    """Row per user → output download event."""
    output = models.ForeignKey(
        DCASOutput,
        on_delete=models.CASCADE,
        related_name="downloads",
        help_text=_("The DCAS CSV that was downloaded.")
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        help_text=_("User who initiated the download.")
    )
    requested_at = models.DateTimeField(
        auto_now_add=True,
        help_text=_("Timestamp when the presigned URL was issued.")
    )

    class Meta:
        db_table = "dcas_download_log"
        ordering = ["-requested_at"]
        verbose_name = _("CSV download")
        verbose_name_plural = _("CSV downloads")

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"{self.user} → {self.output.file_name} "
            f"({self.requested_at:%Y-%m-%d %H:%M})"
        )
